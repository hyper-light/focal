use super::*;
use crate::native::report_tests as f;
use focal_model::{ValidationMode, VerdictValue};

struct Slots<'a>(Vec<EncodedRow<'a>>);
impl<'a> EncodedRows<'a> for Slots<'a> {
    fn len(&self) -> usize {
        self.0.len()
    }
    fn encoded(&self, index: usize) -> Option<&EncodedRow<'a>> {
        self.0.get(index)
    }
}
fn inspect(bytes: &[u8]) -> StructuralRecord<'_> {
    StructuralRecord::inspect(
        bytes,
        InspectionLimits {
            bytes: bytes.len(),
            visits: 100_000_000,
            rows: 100_000,
            row_bytes: 32 << 20,
        },
    )
    .unwrap()
}
fn slots<'a>(record: &StructuralRecord<'a>) -> Slots<'a> {
    Slots(
        record
            .rows(100_000_000)
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap(),
    )
}
fn budget() -> MemoryBudget {
    MemoryBudget::new(16 << 20, 8 << 20).unwrap()
}
fn created(core: &Core<NativeState>) -> NativePrepared {
    f::prepared(core.prepare_native(
        f::context(f::ISSUER, 10),
        f::creation(1, 1, &[(ValidationMode::Required, false)], None),
        &[],
    ))
}

#[test]
fn canonical_slots_and_original_creation_events_are_found_without_predecessor_scans() {
    let core = f::core();
    let candidate = created(&core);
    let bytes = super::super::tests::encode(&candidate);
    let record = inspect(&bytes);
    let slots = slots(&record);
    let memory = budget();
    let parsing = Meter::new(100_000_000);
    let work = Meter::new(100_000_000);
    let index = Index::build(
        &slots,
        record.header(),
        core.limits,
        &memory,
        &parsing,
        &work,
    )
    .unwrap();
    assert_eq!(index.counts.iter().sum::<usize>(), slots.len());
    assert_eq!(index.counts[6], 1);
    assert_eq!(index.counts[1], 2);
    for (at, row) in slots.0.iter().enumerate() {
        assert_eq!(locate(&slots, row.key, &work).unwrap(), Some(at));
    }
    assert_eq!(
        locate(&slots, Key::Claim(ClaimId::from_u128(999)), &work).unwrap(),
        None
    );
    let key = Key::Claim(ClaimId::from_u128(1));
    let mut deleted_bytes = [0u8; 128];
    let used = {
        let mut sink = bytes::SliceSink::new(&mut deleted_bytes, 1000);
        bytes::write_u8(&mut sink, 0).unwrap();
        fixed::key(&mut sink, key).unwrap();
        bytes::write_count(&mut sink, 0).unwrap();
        sink.len()
    };
    let mut cursor = bytes::Cursor::new(&deleted_bytes[..used], used, 1000).unwrap();
    let deleted = Slots(vec![inspect::read_row(&mut cursor, 128).unwrap()]);
    assert!(deleted.encoded(0).unwrap().deleted());
    assert_eq!(locate(&deleted, key, &work).unwrap(), Some(0));
    cursor.finish().unwrap();
    let mut events = Vec::new();
    index
        .events(
            &slots,
            Key::Claim(ClaimId::from_u128(1)),
            &parsing,
            &work,
            |event| {
                events.push(event);
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(
        index
            .first(&slots, Key::Claim(ClaimId::from_u128(1)), &parsing, &work)
            .unwrap(),
        events.first().copied()
    );
    assert!(matches!(
        events[0].fact,
        NativeFact::Claim(NativeClaimEvent {
            kind: NativeEventKind::Created,
            ..
        })
    ));
    assert_eq!(events[0].invocation, candidate.outcome().invocation);
    assert!(memory.stats().used > 0);
    drop(index);
    assert_eq!(memory.stats().used, 0);
}

#[test]
fn real_delivery_publication_is_in_both_result_and_evaluation_histories() {
    let (core, _store, _directory) = super::super::evidence::tests::recovery_fixture(1);
    let claim = core.native_claim(ClaimId::from_u128(1)).unwrap().binding();
    let expected = core
        .native_response(TestamentId::from_u128(900))
        .unwrap()
        .identity()
        .binding;
    let candidate = f::prepared(core.prepare_native(
        f::context(f::ISSUER, 200),
        NativeInput {
            request: f::request(f::ISSUER, 80_001),
            command: NativeCommand::ReceiveResponse { claim, expected },
        },
        &[],
    ));
    let bytes = super::super::tests::encode(&candidate);
    let record = inspect(&bytes);
    let slots = slots(&record);
    let memory = budget();
    let parsing = Meter::new(100_000_000);
    let work = Meter::new(100_000_000);
    let index = Index::build(
        &slots,
        record.header(),
        core.limits,
        &memory,
        &parsing,
        &work,
    )
    .unwrap();
    let result_key = slots
        .0
        .iter()
        .find_map(|row| match row.key {
            Key::DeliveryResult(key) => Some(key),
            _ => None,
        })
        .unwrap();
    let mut history = Vec::new();
    index
        .events(
            &slots,
            Key::Evaluation(result_key.evaluation),
            &parsing,
            &work,
            |event| {
                history.push(event);
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(history.len(), 2);
    assert!(matches!(
        history[0].fact,
        NativeFact::Evaluation {
            kind: NativeEvaluationEventKind::Materialized,
            ..
        }
    ));
    assert_eq!(history[1].fact, NativeFact::Delivery { key: result_key });
    assert!(history[0].ordinal < history[1].ordinal);
    assert_eq!(
        index
            .first(&slots, Key::DeliveryResult(result_key), &parsing, &work)
            .unwrap(),
        Some(history[1])
    );
    let mut registered = 0;
    index
        .registrations(&slots, ClaimId::from_u128(1), &parsing, &work, |_| {
            registered += 1;
            Ok(())
        })
        .unwrap();
    let mut recorded = 0;
    for row in &slots.0 {
        if matches!(row.key, Key::Event(..)) {
            let mut cursor = bytes::Cursor::new(row.body(), row.body().len(), 100_000).unwrap();
            let event = read_events::event(&mut cursor).unwrap();
            cursor.finish().unwrap();
            recorded += usize::from(
                matches!(event.fact, NativeFact::Registrations { claim } if claim.object.0 == ClaimId::from_u128(1).0),
            );
        }
    }
    assert_eq!(registered, recorded);
}

#[test]
fn actual_report_artifact_origin_preserves_request_binding_and_original_ordinal() {
    let core = f::running(&[(ValidationMode::Required, false)]);
    let input = f::report_for(
        &core,
        None,
        4,
        1,
        VerdictValue::Pass,
        f::descriptor(f::artifact_spec(401, f::EVALUATOR, VerdictValue::Pass)),
    );
    let request = input.request;
    let mut custody = f::Custody::new();
    let token = f::verified(&mut custody, &input);
    let candidate = f::report(&core, input, &[], &token);
    let descriptor = candidate
        .artifact(ArtifactId::from_u128(401))
        .unwrap()
        .descriptor();
    let bytes = super::super::tests::encode(&candidate);
    let record = inspect(&bytes);
    let slots = slots(&record);
    let memory = budget();
    let parsing = Meter::new(100_000_000);
    let work = Meter::new(100_000_000);
    let index = Index::build(
        &slots,
        record.header(),
        core.limits,
        &memory,
        &parsing,
        &work,
    )
    .unwrap();
    let origin = index
        .artifact(&slots, descriptor.id(), &parsing, &work)
        .unwrap();
    assert_eq!(origin.request, request);
    assert_eq!(origin.binding, descriptor.binding());
    assert_eq!(
        origin.position,
        PublicationPosition {
            sequence: candidate.outcome().sequence,
            ordinal: 0
        }
    );
    assert!(
        index
            .artifact(&slots, ArtifactId::from_u128(402), &parsing, &work)
            .is_err()
    );
}

#[test]
fn exact_index_funding_and_work_limits_refuse_then_retry_without_retaining_memory() {
    let core = f::core();
    let bytes = super::super::tests::encode(&created(&core));
    let record = inspect(&bytes);
    let slots = slots(&record);
    let memory = budget();
    let parsing = Meter::new(100_000_000);
    let work = Meter::new(100_000_000);
    let index = Index::build(
        &slots,
        record.header(),
        core.limits,
        &memory,
        &parsing,
        &work,
    )
    .unwrap();
    let bytes = memory.stats().used;
    let parser_work = 100_000_000 - parsing.remaining();
    let index_work = 100_000_000 - work.remaining();
    drop(index);
    assert_eq!(memory.stats().used, 0);
    let exact = MemoryBudget::new(bytes, bytes).unwrap();
    let index = Index::build(
        &slots,
        record.header(),
        core.limits,
        &exact,
        &Meter::new(parser_work),
        &Meter::new(index_work),
    )
    .unwrap();
    drop(index);
    assert_eq!(exact.stats().used, 0);
    let short = MemoryBudget::new(bytes - 1, bytes - 1).unwrap();
    assert!(
        Index::build(
            &slots,
            record.header(),
            core.limits,
            &short,
            &Meter::new(parser_work),
            &Meter::new(index_work)
        )
        .is_err()
    );
    assert_eq!(short.stats().used, 0);
    for (parsing, work) in [(parser_work - 1, index_work), (parser_work, index_work - 1)] {
        assert!(
            Index::build(
                &slots,
                record.header(),
                core.limits,
                &exact,
                &Meter::new(parsing),
                &Meter::new(work)
            )
            .is_err()
        );
        assert_eq!(exact.stats().used, 0);
    }
    let mut missing = Slots(
        slots
            .0
            .into_iter()
            .filter(|row| row.key != Key::Claim(ClaimId::from_u128(1)))
            .collect(),
    );
    assert!(
        Index::build(
            &missing,
            record.header(),
            core.limits,
            &exact,
            &Meter::new(parser_work),
            &Meter::new(index_work)
        )
        .is_err()
    );
    assert_eq!(exact.stats().used, 0);
    missing.0.swap(0, 1);
    assert!(
        Index::build(
            &missing,
            record.header(),
            core.limits,
            &exact,
            &Meter::new(parser_work),
            &Meter::new(index_work)
        )
        .is_err()
    );
    assert_eq!(exact.stats().used, 0);
}
