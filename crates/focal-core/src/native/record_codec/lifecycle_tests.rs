use super::*;
use crate::native::{report_tests as f, *};
use bytes::{CountingSink, Cursor, SliceSink};
use focal_model::lifecycle::validation::{AuthorityFence, FenceReason, Suppression};
use focal_model::{
    ArtifactRef, ContentHash, ObjectRevision, ParticipantId, ReceiptFence, ReceiptId, TestamentId,
    ValidationMode, VerdictValue,
};

// Exercise the actual measurement and destination sinks. Both byte and visit
// allowances are exact; one less must refuse even when the other is generous.
macro_rules! encoded {
    ($encode:path, $value:expr) => {{
        let value = $value;
        let mut quote = CountingSink::new(usize::MAX, usize::MAX);
        $encode(&mut quote, value).unwrap();
        let mut output = vec![0; quote.len()];
        let mut sink = SliceSink::new(&mut output, quote.visits_used());
        $encode(&mut sink, value).unwrap();
        assert_eq!(sink.len(), quote.len());
        assert_eq!(sink.visits_used(), quote.visits_used());
        sink.finish().unwrap();
        let mut short = CountingSink::new(quote.len() - 1, usize::MAX);
        assert_eq!($encode(&mut short, value), Err(Error::Capacity));
        let mut short = CountingSink::new(usize::MAX, quote.visits_used() - 1);
        assert_eq!($encode(&mut short, value), Err(Error::Capacity));
        output
    }};
}

fn claim_row(core: &Core<NativeState>) -> &OwnedClaim {
    match core.state.rows.get(&Key::Claim(ClaimId::from_u128(1))) {
        Some(Row::Claim(row)) => row,
        _ => panic!("claim fixture"),
    }
}
fn evaluation_row(core: &Core<NativeState>) -> &OwnedEvaluation {
    match core.state.rows.get(&Key::Evaluation(f::key(1))) {
        Some(Row::Evaluation(row)) => row,
        _ => panic!("evaluation fixture"),
    }
}

#[test]
fn actual_claim_and_evaluation_rows_keep_full_state_and_exact_bounds() {
    let mut core = f::core();
    f::publish(
        &mut core,
        10,
        f::creation(1, 1, &[(ValidationMode::Required, true)], None),
    );
    let generated = encoded!(claim, claim_row(&core));
    let copied = claim_row(&core).copy().unwrap();
    assert_eq!(generated, encoded!(claim, &copied));
    f::publish(&mut core, 20, f::post(2, f::binding(1)));
    assert_ne!(generated, encoded!(claim, claim_row(&core)));
    let ready = encoded!(evaluation, evaluation_row(&core));
    let binding = core.native_claim(ClaimId::from_u128(1)).unwrap().binding();
    let expected = core.native_evaluation(f::key(1)).unwrap().binding();
    f::publish(&mut core, 30, f::begin(3, binding, 1, expected));
    let running = encoded!(evaluation, evaluation_row(&core));
    assert_ne!(ready, running);
    let copied = evaluation_row(&core).copy().unwrap();
    assert_eq!(running, encoded!(evaluation, &copied));
}

#[test]
fn evaluation_retains_every_scalar_and_optional_history_field() {
    let core = f::running(&[(ValidationMode::Required, true)]);
    let base = core
        .native_evaluation(f::key(1))
        .unwrap()
        .snapshot_v1()
        .unwrap();
    let baseline = encoded!(evaluation_snapshot, base);
    // A durable row writer preserves data, without reauthorizing or reducing a
    // supplied snapshot. Distinct sentinel values expose accidentally omitted
    // fields, including cursors too wide for a collection count.
    let changes: &[fn(&mut validation::EvaluationSnapshotV1)] = &[
        |s| s.binding.revision = ObjectRevision(99),
        |s| {
            s.target = validation::Target::Admission {
                claim: f::binding(2),
            }
        },
        |s| s.generation = 17,
        |s| {
            s.receipt = Some(ReceiptFence {
                receipt: ReceiptId::from_u128(33),
                epoch: 9,
            })
        },
        |s| s.state = validation::State::ErroredNotRequired,
        |s| s.phase = validation::Phase::Quality,
        |s| s.handler = u64::MAX,
        |s| s.handler_attempt = 23,
        |s| s.attempt = 29,
        |s| s.begun = false,
        |s| s.suppression = Some(Suppression::ArtifactFailure(ContentHash([44; 32]))),
        |s| s.sealed = Some(ContentHash([45; 32])),
        |s| {
            s.fence = Some(AuthorityFence {
                reason: FenceReason::Revocation,
                cause: ContentHash([46; 32]),
            })
        },
        |s| {
            s.programmatic_evidence = Some(ArtifactRef {
                id: ArtifactId::from_u128(47),
                hash: ContentHash([48; 32]),
            })
        },
        |s| {
            s.last_result = Some(validation::AcceptedResultSnapshotV1 {
                binding: s.binding,
                ledger: s.binding.ledger,
                claim: ClaimId::from_u128(1),
                target: s.target,
                validation: ValidationId::from_u128(101),
                declaration_index: 1,
                mode: ValidationMode::Required,
                verdict: VerdictValue::Error,
                phase: validation::Phase::Programmatic,
                attempt: Some(4),
                generation: s.generation,
                receipt: s.receipt,
                evidence: Some(ArtifactRef {
                    id: ArtifactId::from_u128(50),
                    hash: ContentHash([51; 32]),
                }),
                programmatic_evidence: None,
                reporter: Some(ParticipantId::from_u128(52)),
                resulting_state: validation::State::Errored,
            })
        },
    ];
    for change in changes {
        let mut snapshot = base;
        change(&mut snapshot);
        assert_ne!(baseline, encoded!(evaluation_snapshot, snapshot));
    }
}

#[test]
fn frozen_result_testament_preserves_generated_binding_and_publications_after_post() {
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
    let evidence = f::verified(&mut custody, &input);
    let prepared = f::report(&core, input, &[], &evidence);
    core.publish_native(prepared).unwrap();
    let id = TestamentId::from_u128(950);
    let binding = core.native_claim(ClaimId::from_u128(1)).unwrap().binding();
    let prepared = f::prepared(core.prepare_native(
        f::context(f::ISSUER, 110),
        NativeInput {
            request: f::request(f::ISSUER, 501),
            command: NativeCommand::GenerateResultTestament { claim: binding, id },
        },
        &[],
    ));
    let Some(Row::ResultTestament(row)) = prepared.fragments.get(&Key::ResultTestament(id)) else {
        panic!("generated audit row");
    };
    let generated = row.get().unwrap().generated_binding();
    let before = encoded!(result_testament, row);
    let copy = row.copy().unwrap();
    assert_eq!(before, encoded!(result_testament, &copy));
    let captured = row.get().unwrap().captured_at();
    let generated_at = row.get().unwrap().generated_at();
    let publications = row.get().unwrap().publications().to_vec();
    assert!(!publications.is_empty());
    core.publish_native(prepared).unwrap();
    let prepared = f::prepared(core.prepare_native(
        f::context(f::ISSUER, 120),
        NativeInput {
            request: f::request(f::ISSUER, 502),
            command: NativeCommand::PostResultTestament {
                expected: generated,
            },
        },
        &[],
    ));
    let Some(Row::ResultTestament(row)) = prepared.fragments.get(&Key::ResultTestament(id)) else {
        panic!("posted audit row");
    };
    let value = row.get().unwrap();
    assert_eq!(value.generated_binding(), generated);
    assert_eq!(value.testament().binding(), generated.next().unwrap());
    assert_eq!(value.captured_at(), captured);
    assert_eq!(value.generated_at(), generated_at);
    assert_eq!(value.publications(), publications);
    assert!(value.posted_at().is_some());
    let after = encoded!(result_testament, row);
    assert_ne!(before, after);
    // Original and current bindings occupy distinct fixed-width frames. A
    // decoder can preserve original generation without inferring revision one.
    let binding_bytes = encoded!(types::binding, generated);
    assert_eq!(&before[..binding_bytes.len()], binding_bytes);
    assert_eq!(&after[..binding_bytes.len()], binding_bytes);
    let mut cursor = Cursor::new(&after, after.len(), usize::MAX).unwrap();
    assert_eq!(cursor.take(binding_bytes.len()).unwrap(), binding_bytes);
    let current_bytes = encoded!(types::binding, value.testament().binding());
    assert_eq!(cursor.take(current_bytes.len()).unwrap(), current_bytes);
    assert_eq!(cursor.u8().unwrap(), 1);
    let copy = row.copy().unwrap();
    assert_eq!(after, encoded!(result_testament, &copy));
}

#[test]
fn explicit_state_tags_distinguish_failure_error_and_optional_observation() {
    let values = [
        validation::State::Ready,
        validation::State::Validating,
        validation::State::ValidatingQualityBar,
        validation::State::Validated,
        validation::State::ValidationIncomplete,
        validation::State::ValidationFailed,
        validation::State::ValidationFailedNotRequired,
        validation::State::Errored,
        validation::State::ErroredNotRequired,
        validation::State::QualityBarValidationFailed,
        validation::State::QualityBarValidationFailedNotRequired,
    ];
    for (tag, state) in values.into_iter().enumerate() {
        assert_eq!(
            encoded!(fields::evaluation_state, state),
            [u8::try_from(tag).unwrap()]
        );
    }
    let states = [
        focal_model::ClaimStatus::Generated,
        focal_model::ClaimStatus::Posted,
        focal_model::ClaimStatus::Received,
        focal_model::ClaimStatus::Progressed,
        focal_model::ClaimStatus::TestamentGenerated,
        focal_model::ClaimStatus::TestamentAcknowledged,
        focal_model::ClaimStatus::Validating,
        focal_model::ClaimStatus::Satisfied,
        focal_model::ClaimStatus::PostFailed,
        focal_model::ClaimStatus::ReceiptFailed,
        focal_model::ClaimStatus::TestamentGenerationFailed,
        focal_model::ClaimStatus::ValidationIncomplete,
        focal_model::ClaimStatus::ValidationFailed,
        focal_model::ClaimStatus::ValidationErrored,
        focal_model::ClaimStatus::Cancelled,
        focal_model::ClaimStatus::Expired,
        focal_model::ClaimStatus::Revoked,
        focal_model::ClaimStatus::Superseded,
        focal_model::ClaimStatus::DependencyFailed,
        focal_model::ClaimStatus::Deadlocked,
    ];
    for (index, state) in states.into_iter().enumerate() {
        assert_eq!(
            encoded!(fields::claim_status, state),
            u16::try_from(index + 1).unwrap().to_le_bytes()
        );
    }
}
