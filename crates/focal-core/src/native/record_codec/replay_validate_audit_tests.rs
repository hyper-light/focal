use super::*;
use crate::native::report_tests as f;
use focal_model::{ValidationMode, VerdictValue};
use std::cell::Cell;

struct Rows<'a>(Vec<EncodedRow<'a>>);
impl<'a> replay_index::EncodedRows<'a> for Rows<'a> {
    fn len(&self) -> usize {
        self.0.len()
    }
    fn encoded(&self, index: usize) -> Option<&EncodedRow<'a>> {
        self.0.get(index)
    }
}
struct Source<'a> {
    base: &'a Core<NativeState>,
    candidate: &'a NativePrepared,
    override_before: Option<(Key, Option<&'a Row>)>,
    scans: Cell<usize>,
    lookups: Cell<usize>,
}
impl Overlay for Source<'_> {
    fn before(&self, key: Key) -> Option<&Row> {
        self.lookups.set(self.lookups.get() + 1);
        assert!(
            !matches!(key, Key::Event(..) | Key::Outcome(_)),
            "no complete-history replay"
        );
        if let Some((expected, value)) = self.override_before
            && key == expected
        {
            return value;
        }
        self.base.state.rows.get(&key)
    }
    fn after(&self, key: Key) -> Option<&Row> {
        self.lookups.set(self.lookups.get() + 1);
        self.candidate.range.get(&key)
    }
    fn changes(&self) -> impl ExactSizeIterator<Item = (Key, Option<&Row>)> {
        self.scans.set(self.scans.get() + 1);
        std::iter::empty()
    }
    fn changes_from(&self, _: Key) -> impl Iterator<Item = (Key, Option<&Row>)> {
        self.scans.set(self.scans.get() + 1);
        std::iter::empty()
    }
}
fn candidate(core: &Core<NativeState>) -> NativePrepared {
    f::prepared(core.prepare_native(
        f::context(f::ISSUER, 500),
        NativeInput {
            request: f::request(f::ISSUER, 77_000),
            command: NativeCommand::GenerateResultTestament {
                claim: core.native_claim(ClaimId::from_u128(1)).unwrap().binding(),
                id: TestamentId::from_u128(950),
            },
        },
        &[],
    ))
}
fn inspect(bytes: &[u8]) -> StructuralRecord<'_> {
    StructuralRecord::inspect(
        bytes,
        InspectionLimits {
            bytes: bytes.len(),
            visits: 1_000_000_000,
            rows: 100_000,
            row_bytes: 32 << 20,
        },
    )
    .unwrap()
}
fn check(
    core: &Core<NativeState>,
    candidate: &NativePrepared,
    override_before: Option<(Key, Option<&Row>)>,
    visits: usize,
) -> (Result<(), NativeError>, usize, usize) {
    let bytes = super::super::super::tests::encode(candidate);
    let record = inspect(&bytes);
    let rows = Rows(
        record
            .rows(usize::MAX)
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap(),
    );
    let memory = MemoryBudget::new(1 << 20, 0).unwrap();
    let parsing = Meter::new(1_000_000_000);
    let index = replay_index::Index::build(
        &rows,
        record.header(),
        core.limits,
        &memory,
        &parsing,
        &Meter::new(1_000_000_000),
    )
    .unwrap();
    let work = Meter::new(visits);
    let source = Source {
        base: core,
        candidate,
        override_before,
        scans: Cell::new(0),
        lookups: Cell::new(0),
    };
    let read = ReplayRead {
        overlay: &source,
        encoded: &rows,
        index: &index,
        parsing: &parsing,
        ledger: core.state.ledger,
        profile: core.state.profile,
        base: core.native_sequence(),
        outcome: candidate.outcome(),
        limits: core.limits,
        meter: &work,
        budget: &memory,
    };
    let before = memory.stats();
    let result = generated(
        TestamentId::from_u128(950),
        candidate
            .result_testament(TestamentId::from_u128(950))
            .unwrap(),
        &read,
    );
    assert_eq!(source.scans.get(), 0);
    assert_eq!(memory.stats(), before);
    let count = source.lookups.get();
    drop(index);
    assert_eq!(memory.stats().used, 0);
    (result, visits - work.remaining(), count)
}
fn admission() -> Core<NativeState> {
    let mut core = f::running(&[(ValidationMode::Required, false)]);
    core.limits.plan_edges = 65_536;
    let mut custody = f::Custody::new();
    for (offset, verdict) in [VerdictValue::Error, VerdictValue::Fail]
        .into_iter()
        .enumerate()
    {
        let offset = u128::try_from(offset).unwrap();
        let input = f::report_for(
            &core,
            None,
            60_000 + offset,
            1,
            verdict,
            f::descriptor(f::artifact_spec(61_000 + offset, f::EVALUATOR, verdict)),
        );
        let verified = f::verified(&mut custody, &input);
        let prepared = f::report(&core, input, &[], &verified);
        core.publish_native(prepared).unwrap();
    }
    core
}

#[test]
fn every_actual_accepted_delivery_and_missing_publication_is_frozen_without_history_scans() {
    let mut observed = [false; 3];
    for delivery in [false, true] {
        let core = if delivery {
            let (mut core, _store, _directory) =
                super::super::super::evidence::tests::recovery_fixture(3);
            core.limits.plan_edges = 65_536;
            core
        } else {
            admission()
        };
        let candidate = candidate(&core);
        let value = candidate
            .result_testament(TestamentId::from_u128(950))
            .unwrap();
        for result in value.testament().results() {
            observed[match result.phase() {
                Phase::Delivery => 1,
                Phase::MissingTarget => 2,
                _ => 0,
            }] = true;
        }
        let (result, work, lookups) = check(&core, &candidate, None, 100_000_000);
        result.unwrap();
        assert!(lookups > value.testament().members().len());
        check(&core, &candidate, None, work).0.unwrap();
        assert!(check(&core, &candidate, None, work - 1).0.is_err());
        // Removing any one original result must refuse the complete bundle,
        // including earlier Error evidence before the final failed report.
        for result in value.testament().results() {
            let key = NativeResultKey::of(*result);
            let address = match result.phase() {
                Phase::Delivery => Key::DeliveryResult(key),
                Phase::MissingTarget => Key::MissingResult(key),
                _ => Key::Accepted(key),
            };
            assert!(
                check(&core, &candidate, Some((address, None)), 100_000_000)
                    .0
                    .is_err()
            );
        }
    }
    assert_eq!(observed, [true; 3]);
}

#[test]
fn actual_member_and_original_publication_cannot_be_replaced_with_other_valid_values() {
    let core = admission();
    let candidate = candidate(&core);
    let before = f::running(&[(ValidationMode::Required, false)]);
    let key = Key::Evaluation(f::key(1));
    // The donor is a real earlier state of the same evaluation and policy.
    // Its shape and identity pass, but it is not the frozen predecessor state.
    assert!(
        check(
            &core,
            &candidate,
            Some((key, before.state.rows.get(&key))),
            100_000_000
        )
        .0
        .is_err()
    );
    assert!(
        check(&core, &candidate, Some((key, None)), 100_000_000)
            .0
            .is_err()
    );
    assert!(
        check(
            &core,
            &candidate,
            Some((Key::Claim(ClaimId::from_u128(1)), None)),
            100_000_000
        )
        .0
        .is_err()
    );
    let value = candidate
        .result_testament(TestamentId::from_u128(950))
        .unwrap();
    for result in value.testament().results() {
        let key = NativeResultKey::of(*result);
        let actual = core.native_result(key).unwrap();
        let changed = Row::Accepted(
            OwnedAccepted::new(
                NativeAccepted::new(
                    actual.result(),
                    actual.attempt(),
                    actual.artifact(),
                    actual.sequence(),
                    actual.ordinal() + 1,
                )
                .unwrap(),
            )
            .unwrap(),
        );
        assert!(
            check(
                &core,
                &candidate,
                Some((Key::Accepted(key), Some(&changed))),
                100_000_000
            )
            .0
            .is_err()
        );
    }
}

#[test]
fn revision_coverage_is_bounded_before_probing_an_untrusted_span() {
    let core = admission();
    let candidate = candidate(&core);
    let key = Key::Evaluation(f::key(1));
    let original = core.native_evaluation(f::key(1)).unwrap();
    let declaration = core.native_definition(f::key(1).validation).unwrap();
    let mut snapshot = original.snapshot_v1().unwrap();
    snapshot.binding.revision = ObjectRevision(u64::MAX);
    let inflated =
        validation::EvaluationState::hydrate_v1(declaration, snapshot, usize::MAX).unwrap();
    let inflated = Row::Evaluation(OwnedEvaluation::new(inflated).unwrap());
    let (result, _, lookups) = check(&core, &candidate, Some((key, Some(&inflated))), 100_000_000);
    assert!(result.is_err());
    assert!(lookups < 32);
}
