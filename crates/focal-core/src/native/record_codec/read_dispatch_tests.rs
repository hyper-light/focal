use super::*;
use crate::native::report_tests as f;
use focal_evidence::NativeLocalCustody;
use focal_model::lifecycle::artifact_descriptor::{ArtifactDescriptor, ContentPointer};

struct Empty;
impl Objects for Empty {
    fn ledger(&self) -> LedgerId { f::binding(1).ledger }
    fn prefix(&self) -> SessionSeq { SessionSeq(1) }
    fn get(&self, _key: Key, meter: &Meter) -> Result<Option<&Row>, NativeError> {
        debit(meter, 1)?; Ok(None)
    }
    fn raw_claim(&self, _id: ClaimId, meter: &Meter) -> Result<&[u8], NativeError> {
        debit(meter, 1)?; Err(NativeError::Capacity("original lookup refusal"))
    }
    fn artifact_origin(&self, _id: ArtifactId, meter: &Meter) -> Result<ArtifactOrigin, NativeError> {
        debit(meter, 1)?; Err(invalid())
    }
}
impl evidence::Custody for Empty {
    fn recover(&self, _request: RequestKey, _descriptor: &ArtifactDescriptor,
        _pointer: ContentPointer, _local_revision: u64) -> Result<NativeLocalCustody, NativeError> { Err(invalid()) }
}
fn limits() -> Limits {
    let declaration = validation::Limits { handlers: 16, attempts: 16, slot_bytes: 1024 };
    Limits {
        native: NativeLimits::default(),
        acceptance: aggregation::Limits { max_slots: 16, max_checks: 16, max_results: 32, max_updates: 32 },
        artifact: artifact_descriptor::Limits { kind_bytes: 1024, metadata_bytes: 4096, inline_bytes: 4096,
            inputs: 16, visibility_labels: 16, visibility_label_bytes: 1024, construction_bytes: 65_536 },
        claim: claim_descriptor::Limits { description_bytes: 4096, relations: 16, scopes: 16,
            scope_key_bytes: 1024, requirements: 16, slots: 16, checks: 16, construction_bytes: 65_536 },
        declaration,
        validation: validation_descriptor::Limits { declaration, description_bytes: 4096,
            quality_bar_bytes: 4096, contributors: 16, construction_bytes: 65_536 },
        response: ResponseLimits { artifacts: 16, diagnostics: 16, summary_bytes: 4096, construction_bytes: 65_536 },
        creation_objects: 32,
    }
}
fn wire(key: Key, row: &Row) -> Vec<u8> {
    let mut body = CountingSink::new(usize::MAX, usize::MAX);
    rows::value(&mut body, row, f::binding(1).ledger).unwrap();
    let write = |sink: &mut dyn bytes::Sink| -> Result<(), CodecError> {
        // Sized adapter avoids allocating a body buffer between measurement and
        // enclosing framing; this test intentionally exercises actual writers.
        struct Forward<'a>(&'a mut dyn bytes::Sink);
        impl bytes::Sink for Forward<'_> {
            fn write(&mut self, value: &[u8]) -> Result<(), CodecError> { self.0.write(value) }
            fn visit(&mut self, visits: usize) -> Result<(), CodecError> { self.0.visit(visits) }
        }
        let mut sink = Forward(sink);
        write_u8(&mut sink, 1)?;
        fixed::key(&mut sink, key)?;
        write_count(&mut sink, body.len())?;
        rows::value(&mut sink, row, f::binding(1).ledger)
    };
    let mut size = CountingSink::new(usize::MAX, usize::MAX);
    write(&mut size).unwrap();
    let mut value = vec![0; size.len()];
    let mut sink = SliceSink::new(&mut value, size.visits_used());
    write(&mut sink).unwrap(); sink.finish().unwrap(); value
}
#[test]
fn fixed_dispatch_preserves_present_empty_link_and_exhausts_one_shared_parse_budget() {
    let key = Key::MonitorLink(ClaimId::from_u128(1), focal_model::MonitorId::from_u128(2));
    let bytes = wire(key, &Row::MonitorLink(None));
    let row = RecordRows::new(&bytes, 1, bytes.len(), usize::MAX).unwrap().next().unwrap().unwrap();
    let budget = MemoryBudget::new(65_536, 0).unwrap();
    let parsing = Meter::new(10_000); let source = Meter::new(10_000);
    let model = Meter::new(10_000); let lookup = Meter::new(10_000);
    let context = Context { objects: &Empty, custody: &Empty, workspace: &budget, workspace_lane: BudgetLane::Ordinary,
        parsing: &parsing, source: &source, model: &model, lookup: &lookup, limits: limits() };
    let quote = prepare(&row, &context).unwrap();
    assert_eq!(quote, Quote { heap_bytes: 0, workspace_bytes: 0 });
    let used = 10_000 - parsing.remaining();
    assert!(used > 1);
    with_build(&row, &context, quote, 0, |value, heap| {
        assert!(matches!(value, Row::MonitorLink(None))); assert_eq!(heap, 0); Ok(())
    }).unwrap();
    assert_eq!(10_000 - parsing.remaining(), used * 2);
    assert_eq!(source.remaining(), 10_000);
    assert_eq!(lookup.remaining(), 10_000);
    assert_eq!(budget.stats().used, 0);
    let parsing = Meter::new(used - 1);
    let context = Context { parsing: &parsing, ..context };
    assert!(prepare(&row, &context).is_err());
    assert!(parsing.remaining() < used - 1);
}
#[test]
fn failed_native_parse_keeps_its_original_cause_and_cannot_reset_consumed_work() {
    let meter = Meter::new(8);
    let result: Result<(), NativeError> = parse(&[1, 2], &meter, |cursor| {
        cursor.u8().map_err(evidence::codec)?;
        Err(NativeError::Capacity("precise decoder refusal"))
    });
    assert!(matches!(result, Err(NativeError::Capacity("precise decoder refusal"))));
    assert_eq!(meter.remaining(), 6);
    assert!(parse(&[1, 2], &meter, |cursor| cursor.u8().map_err(evidence::codec)).is_err());
    assert_eq!(meter.remaining(), 4);
}
#[test]
fn model_adapter_preserves_native_lookup_failure_without_heap_or_borrow_panics() {
    struct Refusal;
    impl Objects for Refusal {
        fn ledger(&self) -> LedgerId { f::binding(1).ledger }
        fn prefix(&self) -> SessionSeq { SessionSeq(1) }
        fn get(&self, _key: Key, meter: &Meter) -> Result<Option<&Row>, NativeError> {
            debit(meter, 1)?; Err(NativeError::Capacity("precise restored-store refusal"))
        }
        fn raw_claim(&self, _id: ClaimId, _meter: &Meter) -> Result<&[u8], NativeError> { Err(invalid()) }
        fn artifact_origin(&self, _id: ArtifactId, _meter: &Meter) -> Result<ArtifactOrigin, NativeError> { Err(invalid()) }
    }
    let meter = Meter::new(1024);
    let access = Access::new(&Refusal, &meter);
    let result = read_claim::Objects::declaration(&access, focal_model::ValidationId::from_u128(1));
    assert!(matches!(result, Err(ContractError::Capacity)));
    let result: Result<(), NativeError> = access.finish(Err(ContractError::InvalidManifest.into()));
    assert!(matches!(result, Err(NativeError::Capacity("precise restored-store refusal"))));
    assert_eq!(meter.remaining(), 1024 - 257);
}
