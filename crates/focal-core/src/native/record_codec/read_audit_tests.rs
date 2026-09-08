use super::super::{
    bytes::{CountingSink, SliceSink},
    lifecycle,
};
use super::*;
use crate::native::{report_tests as f, *};
use focal_model::{ContentHash, TestamentId, ValidationMode, VerdictValue};

struct Objects<'a> {
    core: &'a Core<NativeState>,
    wrong_event: bool,
    wrong_result: bool,
}
impl AuditObjects for Objects<'_> {
    fn prefix(&self) -> SessionSeq {
        self.core.native_sequence()
    }
    fn claim(&self, id: ClaimId, meter: &Meter) -> Result<&ClaimState, Error> {
        meter.charge(8192)?;
        self.core
            .native_claim(id)
            .ok_or(Error::InvalidTag("test claim"))
    }
    fn definition(&self, id: ValidationId, meter: &Meter) -> Result<&Declaration, Error> {
        meter.charge(8192)?;
        self.core
            .native_definition(id)
            .ok_or(Error::InvalidTag("test definition"))
    }
    fn result(
        &self,
        key: NativeResultKey,
        meter: &Meter,
    ) -> Result<(AcceptedResult, PublicationPosition), Error> {
        meter.charge(8192)?;
        let value = self
            .core
            .native_result(key)
            .ok_or(Error::InvalidTag("test result"))?;
        Ok((
            value.result(),
            PublicationPosition {
                sequence: value.sequence(),
                ordinal: if self.wrong_result {
                    u32::MAX
                } else {
                    value.ordinal()
                },
            },
        ))
    }
    fn event(&self, at: PublicationPosition, meter: &Meter) -> Result<NativeEvent, Error> {
        meter.charge(8192)?;
        let mut value = self
            .core
            .native_event(at.sequence, at.ordinal)
            .ok_or(Error::InvalidTag("test event"))?;
        if self.wrong_event {
            value.ordinal = u32::MAX;
        }
        Ok(value)
    }
}
fn encoded(row: &OwnedResultTestament) -> Vec<u8> {
    let mut count = CountingSink::new(usize::MAX, usize::MAX);
    lifecycle::result_testament(&mut count, row).unwrap();
    let mut bytes = vec![0; count.len()];
    let mut output = SliceSink::new(&mut bytes, count.visits_used());
    lifecycle::result_testament(&mut output, row).unwrap();
    output.finish().unwrap();
    bytes
}
fn row(core: &Core<NativeState>) -> &OwnedResultTestament {
    let Some(Row::ResultTestament(row)) = core
        .state
        .rows
        .get(&Key::ResultTestament(TestamentId::from_u128(950)))
    else {
        panic!("result testament");
    };
    row
}
fn fixture() -> Core<NativeState> {
    let mut core = f::running(&[(ValidationMode::Required, false)]);
    core.limits.plan_edges = 65_536;
    let mut custody = f::Custody::new();
    let input = f::report_for(
        &core,
        None,
        500,
        1,
        VerdictValue::Fail,
        f::descriptor(f::artifact_spec(1500, f::EVALUATOR, VerdictValue::Fail)),
    );
    let token = f::verified(&mut custody, &input);
    let prepared = f::report(&core, input, &[], &token);
    core.publish_native(prepared).unwrap();
    let binding = core.native_claim(ClaimId::from_u128(1)).unwrap().binding();
    f::publish(
        &mut core,
        110,
        NativeInput {
            request: f::request(f::ISSUER, 501),
            command: NativeCommand::GenerateResultTestament {
                claim: binding,
                id: TestamentId::from_u128(950),
            },
        },
    );
    core
}
fn read(bytes: &[u8]) -> AuditBody<'_> {
    let mut cursor = Cursor::new(bytes, bytes.len(), usize::MAX).unwrap();
    let body = AuditBody::read(&mut cursor).unwrap();
    cursor.finish().unwrap();
    body
}

#[test]
fn actual_generated_and_posted_audits_restore_identically_under_exact_quotes() {
    let mut core = fixture();
    for stage in 0..2 {
        let bytes = encoded(row(&core));
        let body = read(&bytes);
        let quote = body.quote().unwrap();
        let objects = Objects {
            core: &core,
            wrong_event: false,
            wrong_result: false,
        };
        let meter = Meter::new(10_000_000);
        let (restored, actual) = body.build(&objects, quote, &meter).unwrap();
        let visits = 10_000_000 - meter.remaining();
        assert_eq!(encoded(&restored), bytes);
        assert_eq!(actual, restored.heap_charge().unwrap());
        assert_eq!(actual, quote.retained_bytes);
        assert_eq!(restored.get().unwrap().generated_binding(), body.generated);
        let (exact, _) = body.build(&objects, quote, &Meter::new(visits)).unwrap();
        assert_eq!(encoded(&exact), bytes);
        assert!(matches!(
            body.build(&objects, quote, &Meter::new(visits - 1)),
            Err(Error::Capacity)
        ));
        for allowance in [
            AuditQuote {
                workspace_bytes: quote.workspace_bytes - 1,
                ..quote
            },
            AuditQuote {
                retained_bytes: quote.retained_bytes - 1,
                ..quote
            },
        ] {
            assert!(matches!(
                body.build(&objects, allowance, &Meter::new(10_000_000)),
                Err(Error::Capacity)
            ));
        }
        if stage == 0 {
            let expected = row(&core).get().unwrap().testament().binding();
            f::publish(
                &mut core,
                120,
                NativeInput {
                    request: f::request(f::ISSUER, 502),
                    command: NativeCommand::PostResultTestament { expected },
                },
            );
        }
    }
}

#[test]
fn original_events_and_result_publications_cannot_be_substituted() {
    let core = fixture();
    let bytes = encoded(row(&core));
    let body = read(&bytes);
    let quote = body.quote().unwrap();
    for (wrong_event, wrong_result) in [(true, false), (false, true)] {
        let objects = Objects {
            core: &core,
            wrong_event,
            wrong_result,
        };
        assert!(
            body.build(&objects, quote, &Meter::new(10_000_000))
                .is_err()
        );
    }
    let mut changed = body;
    changed.generated.revision = ObjectRevision(7);
    changed.coordinates.generated = changed.generated;
    let objects = Objects {
        core: &core,
        wrong_event: false,
        wrong_result: false,
    };
    assert!(
        changed
            .build(&objects, changed.quote().unwrap(), &Meter::new(10_000_000))
            .is_err()
    );
    let mut changed = body;
    changed.snapshot.cohort.result_capacity = 0;
    assert!(changed.quote().is_err());
    for end in 0..bytes.len() {
        let prefix = &bytes[..end];
        let mut cursor = Cursor::new(prefix, prefix.len(), usize::MAX).unwrap();
        assert!(AuditBody::read(&mut cursor).is_err());
    }
}

#[test]
fn native_recovery_recomputes_the_complete_bundle_content() {
    let core = fixture();
    let source = row(&core).get().unwrap();
    let source_bytes = encoded(row(&core));
    let body = read(&source_bytes);
    let quote = body.quote().unwrap();
    let testament = source
        .testament()
        .try_copy(source.testament().copy_charge().unwrap())
        .unwrap();
    let mut publications = source.publications().to_vec();
    publications[0].position.ordinal += 1;
    let visits = OwnedResultTestament::recovery_visits(
        testament.cohort().members().len(),
        testament.results().len(),
        publications.len(),
    )
    .unwrap();
    assert!(matches!(
        OwnedResultTestament::recover(
            testament,
            publications,
            body.coordinates,
            quote.retained_bytes,
            visits
        ),
        Err(NativeError::Contract(ContractError::ContentConflict))
    ));
    // A hash pin supplied by the byte stream never substitutes for the actual
    // retained cohort content or original coordinates.
    assert_ne!(body.generated.content, ContentHash([0; 32]));
}
