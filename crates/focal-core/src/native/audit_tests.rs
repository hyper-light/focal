use crate::native::report_tests as fixture;
use crate::native::*;
use focal_memory::{BudgetKind, BudgetLane, Change, Entry};
use focal_model::lifecycle::{
    aggregation::PublicationPosition, artifact_descriptor::ResultProvenance,
};
use focal_model::{ArtifactRef, ObjectRevision, ValidationMode, VerdictValue};
use std::cell::Cell;

const CLAIM: ClaimId = ClaimId::from_u128(1);
const RESPONSE: TestamentId = TestamentId::from_u128(900);

fn report_input(owner: &NativeOwner, index: u32, id: u128, value: VerdictValue) -> NativeInput {
    let view = owner.effective();
    let key = fixture::key(index);
    let state = view.evaluation(key).unwrap();
    let attempt = state
        .bind(view.definition(key.validation).unwrap())
        .unwrap()
        .current_attempt()
        .unwrap();
    let artifact = fixture::descriptor(fixture::artifact_spec(
        10_000 + id,
        attempt.evaluator,
        value,
    ))
    .with_result_provenance(ResultProvenance {
        claim: CLAIM,
        validation: key.validation,
        target: state.target(),
        generation: state.generation(),
        attempt,
        value,
    })
    .unwrap();
    NativeInput {
        request: fixture::request(attempt.evaluator, id),
        command: NativeCommand::ReportAdmission {
            claim: view.claim(CLAIM).unwrap().binding(),
            key,
            expected: state.binding(),
            report: validation::Report {
                generation: state.generation(),
                attempt,
                value,
                evidence: ArtifactRef {
                    id: artifact.id(),
                    hash: artifact.content_hash(),
                },
            },
            artifact: NativeArtifactInput::new(artifact).unwrap(),
        },
    }
}

struct Fixture {
    owner: NativeOwner,
    custody: fixture::Custody,
    serial: u128,
    history: Vec<NativeAccepted>,
}

impl Fixture {
    fn new() -> Self {
        let mut core = fixture::core();
        core.limits.plan_edges = 65_536;
        fixture::publish(
            &mut core,
            10,
            fixture::creation(
                1,
                1,
                &[
                    (ValidationMode::Required, true),
                    (ValidationMode::Observe, true),
                    (ValidationMode::Observe, false),
                ],
                None,
            ),
        );
        fixture::publish(&mut core, 20, fixture::post(2, fixture::binding(1)));
        let mut owner = NativeOwner::new(core).unwrap();
        for index in [1, 2] {
            let input = fixture::begin(
                10 + u128::from(index),
                owner.effective().claim(CLAIM).unwrap().binding(),
                index,
                owner
                    .effective()
                    .evaluation(fixture::key(index))
                    .unwrap()
                    .binding(),
            );
            let NativeStaging::Prepared { candidate, .. } = owner
                .prepare(fixture::context(fixture::EVALUATOR, 30), input, None)
                .unwrap()
            else {
                panic!("actual Begin")
            };
            owner.publish_after_durable(candidate).unwrap();
        }
        Self {
            owner,
            custody: fixture::Custody::new(),
            serial: 500,
            history: Vec::new(),
        }
    }

    fn report(
        &mut self,
        index: u32,
        value: VerdictValue,
        publish: bool,
    ) -> (NativeCandidate, NativeOutcome) {
        self.serial += 1;
        let input = report_input(&self.owner, index, self.serial, value);
        let evidence = fixture::verified(&mut self.custody, &input);
        let NativeStaging::Prepared { candidate, outcome } = self
            .owner
            .prepare(
                fixture::context(input.request.principal, 100),
                input,
                Some(&evidence),
            )
            .unwrap()
        else {
            panic!("actual report")
        };
        let view = self.owner.candidate(candidate).unwrap();
        let result = view
            .evaluation(fixture::key(index))
            .unwrap()
            .last_result()
            .unwrap();
        self.history
            .push(*view.result(NativeResultKey::of(result)).unwrap());
        if publish {
            self.owner.publish_after_durable(candidate).unwrap();
        }
        (candidate, outcome)
    }

    fn assert_audit(&self, complete: bool, sealed_at: SessionSeq, captured_at: SessionSeq) {
        let before_budget = self.owner.budget_stats();
        let before_range = self.owner.range_stats();
        self.owner
            .with_effective_audit(CLAIM, |audit| {
                assert_eq!(audit.captured_at(), captured_at);
                assert_eq!(audit.cohort().sealed_at(), sealed_at);
                assert_eq!(audit.cohort().complete(), complete);
                assert_eq!(audit.cohort().result_count(), self.history.len());
                assert_eq!(audit.cohort().members().len(), 3);
                assert_eq!(audit.publications().len(), self.history.len());
                assert!(
                    audit
                        .publications()
                        .windows(2)
                        .all(|pair| pair[0].key < pair[1].key)
                );
                for accepted in &self.history {
                    let expected = PublicationPosition {
                        sequence: accepted.sequence(),
                        ordinal: accepted.ordinal(),
                    };
                    assert_eq!(audit.publication(accepted.result()), Some(expected));
                    let published = audit
                        .publications()
                        .iter()
                        .find(|publication| {
                            publication.key == NativeResultKey::of(accepted.result())
                        })
                        .unwrap();
                    assert_eq!(published.position, expected);
                }
                let suppressed = audit
                    .cohort()
                    .members()
                    .iter()
                    .find(|member| member.key().validation == fixture::key(3).validation)
                    .unwrap();
                assert_eq!(suppressed.state(), validation::State::Ready);
                assert!(matches!(
                    suppressed.suppression(),
                    Some(validation::Suppression::CohortSealed(_))
                ));
                assert!(suppressed.complete());
            })
            .unwrap();
        assert_eq!(self.owner.budget_stats(), before_budget);
        assert_eq!(self.owner.range_stats(), before_range);
    }
}

#[test]
fn audit_reads_actual_pending_and_committed_retry_quality_and_late_observe_history() {
    let mut f = Fixture::new();
    f.report(1, VerdictValue::Error, true);
    f.report(1, VerdictValue::Pass, true);
    let committed_before_seal = f.owner.committed().sequence();
    let (candidate, sealed) = f.report(1, VerdictValue::Fail, false);
    assert_eq!(
        f.owner.effective().claim(CLAIM).unwrap().status(),
        ClaimStatus::PostFailed
    );
    assert_eq!(f.owner.committed().sequence(), committed_before_seal);
    assert!(f.owner.with_committed_audit(CLAIM, |_| ()).is_err());
    f.assert_audit(false, sealed.sequence, sealed.sequence);
    f.owner.publish_after_durable(candidate).unwrap();
    f.owner
        .with_committed_audit(CLAIM, |audit| {
            assert!(!audit.cohort().complete());
            assert_eq!(audit.cohort().result_count(), 3);
            assert_eq!(audit.captured_at(), sealed.sequence);
        })
        .unwrap();
    let original_claim = f.owner.committed().claim(CLAIM).unwrap().binding();
    let original_seal = f.owner.committed().claim(CLAIM).unwrap().local_sealed_at();

    let (candidate, late_error) = f.report(2, VerdictValue::Error, false);
    let observed = f.history.last().unwrap();
    assert_eq!(observed.result().attempt(), Some(0));
    assert_eq!(observed.result().binding().revision, ObjectRevision(4));
    assert!(observed.sequence() > sealed.sequence);
    assert!(
        f.owner
            .committed()
            .result(NativeResultKey::of(observed.result()))
            .is_none()
    );
    f.assert_audit(false, sealed.sequence, late_error.sequence);
    f.owner
        .with_committed_audit(CLAIM, |audit| {
            assert_eq!(audit.cohort().result_count(), 3);
            assert_eq!(audit.publication(observed.result()), None);
        })
        .unwrap();
    f.owner.publish_after_durable(candidate).unwrap();
    let (_, late_pass) = f.report(2, VerdictValue::Pass, true);
    f.assert_audit(false, sealed.sequence, late_pass.sequence);
    let (_, quality) = f.report(2, VerdictValue::Pass, true);
    f.assert_audit(true, sealed.sequence, quality.sequence);
    assert_eq!(
        f.owner.committed().claim(CLAIM).unwrap().binding(),
        original_claim
    );
    assert_eq!(
        f.owner.committed().claim(CLAIM).unwrap().local_sealed_at(),
        original_seal
    );
    assert_eq!(
        f.owner.committed().claim(CLAIM).unwrap().status(),
        ClaimStatus::PostFailed
    );
    assert_eq!(
        f.history
            .iter()
            .map(|accepted| accepted.result().phase())
            .collect::<Vec<_>>(),
        [
            validation::Phase::Programmatic,
            validation::Phase::Programmatic,
            validation::Phase::Quality,
            validation::Phase::Programmatic,
            validation::Phase::Programmatic,
            validation::Phase::Quality
        ]
    );
    assert_eq!(
        f.history
            .iter()
            .map(|accepted| accepted.attempt().evaluator)
            .collect::<Vec<_>>(),
        [
            fixture::EVALUATOR,
            fixture::EVALUATOR,
            fixture::QUALITY,
            fixture::EVALUATOR,
            fixture::EVALUATOR,
            fixture::QUALITY
        ]
    );
}

#[test]
fn audit_preserves_delivery_and_missing_positions_without_result_artifacts_and_discards_pending_entry()
 {
    let core = work_authority::history_fixture(false, true);
    let mut owner = NativeOwner::new(core).unwrap();
    let before_budget = owner.budget_stats();
    let before_range = owner.range_stats();
    let claim = owner.effective().claim(CLAIM).unwrap().binding();
    let response = owner
        .effective()
        .response(RESPONSE)
        .unwrap()
        .identity()
        .binding;
    let before_sequence = owner.effective().sequence();
    for publish in [false, true] {
        let NativeStaging::Prepared { candidate, outcome } = owner
            .prepare(
                fixture::context(fixture::ISSUER, 200),
                NativeInput {
                    request: fixture::request(fixture::ISSUER, 9002),
                    command: NativeCommand::EnterWholeWork {
                        claim,
                        expected: response,
                    },
                },
                None,
            )
            .unwrap()
        else {
            panic!("entry")
        };
        let expected = owner
            .effective()
            .registrations(CLAIM)
            .unwrap()
            .rows()
            .iter()
            .filter_map(|registered| {
                let key = transactions::key_for_registered(CLAIM, *registered);
                let result = owner.effective().evaluation(key).unwrap().last_result()?;
                let position = match result.phase() {
                    validation::Phase::Delivery => {
                        let record = owner
                            .effective()
                            .delivery_result(NativeResultKey::of(result))
                            .unwrap();
                        PublicationPosition {
                            sequence: record.sequence(),
                            ordinal: record.ordinal(),
                        }
                    }
                    validation::Phase::MissingTarget => {
                        let record = owner
                            .effective()
                            .missing_result(NativeResultKey::of(result))
                            .unwrap();
                        PublicationPosition {
                            sequence: record.sequence(),
                            ordinal: record.ordinal(),
                        }
                    }
                    _ => panic!("structural result"),
                };
                Some((result, position))
            })
            .collect::<Vec<_>>();
        assert_eq!(expected.len(), 2);
        owner
            .with_effective_audit(CLAIM, |audit| {
                assert!(audit.cohort().complete());
                assert_eq!(audit.cohort().members().len(), 3);
                assert_eq!(audit.cohort().result_count(), 2);
                assert_eq!(audit.publications().len(), 2);
                assert_eq!(audit.captured_at(), outcome.sequence);
                assert_eq!(audit.cohort().sealed_at(), outcome.sequence);
                for (result, position) in &expected {
                    assert_eq!(result.attempt(), None);
                    assert_eq!(result.evidence(), None);
                    assert_eq!(result.reporter(), None);
                    assert_eq!(audit.publication(*result), Some(*position));
                    match result.phase() {
                        validation::Phase::Delivery => {
                            assert!(position.sequence <= before_sequence)
                        }
                        validation::Phase::MissingTarget => {
                            assert_eq!(position.sequence, outcome.sequence)
                        }
                        _ => panic!("structural phase"),
                    }
                }
                let suppressed = audit
                    .cohort()
                    .members()
                    .iter()
                    .find(|member| {
                        member.suppression() == Some(validation::Suppression::MissingTarget)
                    })
                    .unwrap();
                assert_eq!(suppressed.state(), validation::State::Ready);
            })
            .unwrap();
        assert!(owner.with_committed_audit(CLAIM, |_| ()).is_err());
        assert_eq!(outcome.artifacts, 0);
        if publish {
            owner.publish_after_durable(candidate).unwrap();
            owner
                .with_committed_audit(CLAIM, |audit| assert!(audit.cohort().complete()))
                .unwrap();
        } else {
            owner.discard_from(candidate).unwrap();
            assert!(owner.with_effective_audit(CLAIM, |_| ()).is_err());
            assert_eq!(owner.budget_stats(), before_budget);
            assert_eq!(owner.range_stats(), before_range);
        }
    }
}

#[test]
fn audit_refuses_open_claim_or_ordinary_pressure_before_callback_and_releases_query_buffers() {
    let mut f = Fixture::new();
    let called = Cell::new(false);
    let before = f.owner.budget_stats();
    assert!(
        f.owner
            .with_effective_audit(CLAIM, |_| called.set(true))
            .is_err()
    );
    assert!(!called.get());
    assert_eq!(f.owner.budget_stats(), before);
    f.report(1, VerdictValue::Error, true);
    f.report(1, VerdictValue::Pass, true);
    let (_, sealed) = f.report(1, VerdictValue::Fail, true);
    let parent = f.owner.budget_for_test();
    let pressure = parent
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            parent.limit() - parent.stats().used,
        )
        .unwrap()
        .commit();
    let pressured = f.owner.budget_stats();
    assert!(
        f.owner
            .with_effective_audit(CLAIM, |_| called.set(true))
            .is_err()
    );
    assert!(!called.get());
    assert_eq!(f.owner.budget_stats(), pressured);
    drop(pressure);
    f.assert_audit(false, sealed.sequence, sealed.sequence);
}

fn completed_core() -> (Core<NativeState>, fixture::Custody, Vec<NativeAccepted>) {
    let mut core = fixture::running(&[(ValidationMode::Required, true)]);
    core.limits.plan_edges = 65_536;
    let mut custody = fixture::Custody::new();
    let mut history = Vec::new();
    for (offset, value) in [VerdictValue::Error, VerdictValue::Pass, VerdictValue::Fail]
        .into_iter()
        .enumerate()
    {
        let index = fixture::key(1);
        let state = core.native_evaluation(index).unwrap();
        let attempt = state
            .bind(core.native_definition(index.validation).unwrap())
            .unwrap()
            .current_attempt()
            .unwrap();
        let id = 7000 + u128::try_from(offset).unwrap();
        let input = fixture::report_for(
            &core,
            None,
            id,
            1,
            value,
            fixture::descriptor(fixture::artifact_spec(id, attempt.evaluator, value)),
        );
        let evidence = fixture::verified(&mut custody, &input);
        let prepared = fixture::report(&core, input, &[], &evidence);
        let result = prepared.evaluation(index).unwrap().last_result().unwrap();
        history.push(*prepared.result(NativeResultKey::of(result)).unwrap());
        core.publish_native(prepared).unwrap();
    }
    (core, custody, history)
}

#[test]
fn audit_rejects_corrupt_history_publications_and_artifact_identity() {
    for corruption in 0..5 {
        let (mut core, _custody, history) = completed_core();
        core.with_native_audit(CLAIM, |audit| {
            assert!(audit.cohort().complete());
            assert_eq!(audit.cohort().result_count(), 3);
        })
        .unwrap();
        let first = history[0];
        let first_key = NativeResultKey::of(first.result());
        let change = match corruption {
            0 => Change::Delete(Key::Accepted(first_key)),
            1 => {
                let row = OwnedAccepted::new(first).unwrap();
                let heap = row.heap_charge().unwrap();
                Change::Put(Entry::new(
                    Key::Accepted(NativeResultKey {
                        revision: ObjectRevision(u64::MAX),
                        ..first_key
                    }),
                    Row::Accepted(row),
                    heap,
                ))
            }
            2 => {
                let mut event = core
                    .native_event(first.sequence(), first.ordinal())
                    .unwrap();
                event.fact = NativeFact::Registrations {
                    claim: core.native_claim(CLAIM).unwrap().binding(),
                };
                let row = OwnedEvent::new(StoredEvent::pack(event).unwrap()).unwrap();
                let heap = row.heap_charge().unwrap();
                Change::Put(Entry::new(
                    Key::Event(first.sequence(), first.ordinal()),
                    Row::Event(row),
                    heap,
                ))
            }
            3 => {
                let accepted = NativeAccepted::new(
                    first.result(),
                    first.attempt(),
                    first.artifact(),
                    SessionSeq(core.native_sequence().0 + 2),
                    first.ordinal(),
                )
                .unwrap();
                let row = OwnedAccepted::new(accepted).unwrap();
                let heap = row.heap_charge().unwrap();
                Change::Put(Entry::new(
                    Key::Accepted(first_key),
                    Row::Accepted(row),
                    heap,
                ))
            }
            4 => {
                let evidence = first.result().evidence().unwrap();
                let original = core.native_artifact(evidence.id).unwrap();
                let descriptor = fixture::descriptor(fixture::artifact_spec(
                    99_999,
                    first.attempt().evaluator,
                    first.result().verdict(),
                ))
                .with_result_provenance(original.descriptor().result_provenance().unwrap())
                .unwrap();
                // Artifact IDs are absent from the descriptor content hash.
                // A matching payload and provenance cannot replace identity.
                assert_ne!(descriptor.id(), evidence.id);
                assert_eq!(descriptor.content_hash(), evidence.hash);
                assert_eq!(
                    descriptor.result_provenance(),
                    original.descriptor().result_provenance(),
                );
                let mut facts = original.facts().unwrap();
                facts.binding = descriptor.binding();
                let artifact = NativeArtifact::new(descriptor, original.custody(), facts).unwrap();
                let row = OwnedArtifact::new(artifact).unwrap();
                let heap = row.heap_charge().unwrap();
                Change::Put(Entry::new(
                    Key::Artifact(evidence.id),
                    Row::Artifact(row),
                    heap,
                ))
            }
            _ => unreachable!(),
        };
        let range = core
            .state
            .rows
            .prepare_batch_with(
                core.native_sequence().0 + 1,
                vec![change],
                BudgetLane::Ordinary,
                prepare::copy,
            )
            .unwrap();
        core.state.rows.publish(range).unwrap();
        let before = core.native_budget();
        let called = Cell::new(false);
        assert!(
            core.with_native_audit(CLAIM, |_| called.set(true)).is_err(),
            "corruption {corruption}"
        );
        assert!(!called.get());
        assert_eq!(core.native_budget(), before);
    }
}
