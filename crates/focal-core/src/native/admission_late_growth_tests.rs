//! A later response may enlarge the registry beyond Admission's initial scan
//! bound. Its already-funded Admission attempt remains independent audit work.
use super::super::report_tests::{EVALUATOR, artifact_spec, key};
use super::*;
use focal_memory::{BudgetKind, BudgetLane};
use focal_model::VerdictValue;
use focal_model::lifecycle::artifact_descriptor::ResultProvenance;

struct Projection<'a> {
    view: &'a NativeView<'a>,
    claim: ClaimId,
}
impl aggregation::AdmissionView for Projection<'_> {
    fn prefix(&self) -> SessionSeq {
        self.view.sequence()
    }
    fn declaration(&self, id: ValidationId) -> Option<&validation::Declaration> {
        self.view.definition(id)
    }
    fn evaluation(
        &self,
        row: aggregation::RegisteredEvaluation,
    ) -> Option<&validation::EvaluationState> {
        self.view
            .evaluation(super::super::transactions::key_for_registered(
                self.claim, row,
            ))
    }
    fn accepted(
        &self,
        result: &validation::AcceptedResult,
    ) -> Option<aggregation::PublishedAdmissionResult<'_>> {
        let accepted = self.view.result(NativeResultKey::of(*result))?;
        Some(aggregation::PublishedAdmissionResult {
            result: accepted.result_ref(),
            sequence: accepted.sequence(),
            ordinal: accepted.ordinal(),
        })
    }
}

fn fixture() -> (Fixture, MemoryBudget) {
    let budget = MemoryBudget::new(128 * 1024 * 1024, 16 * 1024 * 1024).unwrap();
    let core = Core::new_native(
        binding(1).ledger,
        RangeId(3781),
        NativeLimits {
            plan_nodes: 16,
            plan_edges: 4096,
            preparation_bytes: 1024 * 1024,
            evaluations_per_claim: 256,
            range: RangeConfig {
                // Fifty definitions and their index rows in one creation.
                max_batch_entries: 256,
                page_entries: 4,
                ..RangeConfig::default()
            },
            ..NativeLimits::default()
        },
        budget.clone(),
    )
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let store = ContentStore::open(
        directory.path(),
        StoreLimits {
            max_content_bytes: 2 * 1024 * 1024,
            max_staging_bytes: 4 * 1024 * 1024,
            max_uploads: 8,
            chunk_bytes: 17,
            max_manifest_bytes: 128 * 1024,
        },
    )
    .unwrap();
    let mut f = Fixture {
        owner: NativeOwner::new(core).unwrap(),
        store,
        _directory: directory,
        serial: 10,
    };
    let mut input = creation(1, 1, &[(ValidationMode::Observe, false)], None);
    let NativeCommand::Create {
        claims,
        declarations,
    } = &mut input.command
    else {
        panic!("actual creation");
    };
    // Exactly fifty immutable declarations: Receipt, Observe Admission, and
    // forty-eight distinct WholeWork checks on the same real output slot.
    for index in 2..50 {
        declarations.push(super::work_check_tests::declaration(
            index,
            0,
            ValidationMode::Required,
            1000,
        ));
    }
    let checks: Vec<_> = declarations
        .iter()
        .skip(2)
        .map(|declaration| aggregation::CheckPolicy {
            declaration_index: declaration.declaration_index(),
            validation: ValidationId(declaration.binding().object.0),
            mode: declaration.mode(),
        })
        .collect();
    claims[0].definition.acceptance = aggregation::AcceptancePolicy::new(
        binding(1),
        ISSUER,
        &[aggregation::SlotPolicy {
            slot: 0,
            missing_declaration_index: 60,
            mode: ValidationMode::Required,
            checks: &checks,
        }],
        declarations,
        aggregation::Limits {
            max_slots: 2,
            max_checks: 64,
            max_results: 256,
            max_updates: 16,
        },
    )
    .unwrap();
    f.commit(ISSUER, input.command);
    f.commit(
        ISSUER,
        NativeCommand::Post {
            expected: f.claim(),
        },
    );
    (f, budget)
}

#[test]
fn funded_admission_finishes_after_large_work_cohort_receipt_under_full_parent_pressure() {
    let (mut f, budget) = fixture();
    let id = ClaimId::from_u128(1);
    let admission = key(1);
    {
        let view = f.owner.committed();
        let claim = view.claim(id).unwrap();
        let registry = view.registrations(id).unwrap();
        assert_eq!(claim.acceptance().declarations().len(), 50);
        assert_eq!(registry.rows().len(), 1);
        assert_eq!(
            aggregation::admission_completion_visits(claim, registry).unwrap(),
            2752
        );
    }
    let expected = f.owner.effective().evaluation(admission).unwrap().binding();
    f.commit(
        EVALUATOR,
        NativeCommand::BeginAdmission {
            claim: f.claim(),
            key: admission,
            expected,
        },
    );
    let saved = *f.owner.committed().evaluation(admission).unwrap();
    let saved_attempt = saved
        .bind(
            f.owner
                .committed()
                .definition(admission.validation)
                .unwrap(),
        )
        .unwrap()
        .current_attempt()
        .unwrap();
    f.commit(
        SUBJECT,
        NativeCommand::AcquireReceipt {
            expected: f.claim(),
            receipt: ReceiptId::from_u128(701),
        },
    );
    let output = f.work(801, 0);
    f.commit(
        SUBJECT,
        f.close(900, OutcomeKind::Complete, vec![output], vec![]),
    );
    f.commit(
        SUBJECT,
        NativeCommand::PostResponse {
            claim: f.claim(),
            expected: f.response(900),
        },
    );
    f.commit(
        ISSUER,
        NativeCommand::ReceiveResponse {
            claim: f.claim(),
            expected: f.response(900),
        },
    );
    let (claim_before, response_before, work_before, report) = {
        let view = f.owner.committed();
        let claim = view.claim(id).unwrap();
        let registry = view.registrations(id).unwrap();
        assert_eq!(registry.rows().len(), 50);
        assert_eq!(
            aggregation::admission_completion_visits(claim, registry).unwrap(),
            5300
        );
        // Exercise the actual old projection, not just its arithmetic quote.
        // It is no longer part of late-report authorization or publication.
        let rows = Projection {
            view: &view,
            claim: id,
        };
        assert_eq!(
            aggregation::project_admission(
                claim,
                registry,
                &rows,
                aggregation::AdmissionLimits {
                    declarations: 4_000_000,
                    evaluations: 256,
                    visits: 4096
                }
            )
            .unwrap_err(),
            ContractError::Capacity
        );
        for row in registry
            .rows()
            .iter()
            .filter(|row| matches!(row.target(), validation::Target::Artifact { .. }))
        {
            let key = super::super::transactions::key_for_registered(id, *row);
            let state = view.evaluation(key).unwrap();
            assert_eq!(state.state(), validation::State::Ready);
            assert!(!state.has_begun());
            assert!(state.last_result().is_none());
        }
        assert_eq!(*view.evaluation(admission).unwrap(), saved);
        let descriptor = descriptor(artifact_spec(802, EVALUATOR, VerdictValue::Pass))
            .with_result_provenance(ResultProvenance {
                claim: id,
                validation: admission.validation,
                target: saved.target(),
                generation: saved.generation(),
                attempt: saved_attempt,
                value: VerdictValue::Pass,
            })
            .unwrap();
        let report = NativeCommand::ReportAdmission {
            claim: claim.binding(),
            key: admission,
            expected: saved.binding(),
            report: validation::Report {
                generation: saved.generation(),
                attempt: saved_attempt,
                value: VerdictValue::Pass,
                evidence: ArtifactRef {
                    id: descriptor.id(),
                    hash: descriptor.content_hash(),
                },
            },
            artifact: NativeArtifactInput::new(descriptor).unwrap(),
        };
        let response = view.response(TestamentId::from_u128(900)).unwrap();
        (
            claim.try_copy(claim.retained_bytes().unwrap()).unwrap(),
            response
                .try_copy(response.retained_bytes().unwrap())
                .unwrap(),
            view.work(output.artifact.id).unwrap().state,
            report,
        )
    };
    let pin = f.owner.pin(0, 100).unwrap();
    let pressure = budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            budget.limit() - budget.stats().used,
        )
        .unwrap()
        .commit();
    assert_eq!(budget.stats().used, budget.limit());
    assert!(
        budget
            .reserve(BudgetKind::Pending, BudgetLane::Completion, 1)
            .is_err()
    );
    let outcome = f.commit(EVALUATOR, report);
    assert_eq!(outcome.events, 3);
    assert_eq!(outcome.changed, 0);
    let view = f.owner.committed();
    assert_eq!(view.claim(id), Some(&claim_before));
    assert_eq!(
        view.response(TestamentId::from_u128(900)),
        Some(&response_before)
    );
    assert_eq!(view.work(output.artifact.id).unwrap().state, work_before);
    let state = view.evaluation(admission).unwrap();
    assert_eq!(state.state(), validation::State::Validated);
    let result = view
        .result(NativeResultKey {
            evaluation: admission,
            revision: state.binding().revision,
        })
        .unwrap();
    assert_eq!(result.attempt(), saved_attempt);
    assert_eq!(result.result().verdict(), VerdictValue::Pass);
    assert!(view.artifact(ArtifactId::from_u128(802)).is_some());
    assert_eq!(
        pin.with_evaluation(admission, 0, |state| state.state())
            .unwrap(),
        Some(validation::State::Validating)
    );
    drop(pressure);
    f.owner.release(&pin).unwrap();
}
