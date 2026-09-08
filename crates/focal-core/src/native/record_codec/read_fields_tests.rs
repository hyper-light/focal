use super::super::{
    bytes::{CountingSink, SliceSink},
    lifecycle, lifecycle_fields as write, types,
};
use super::*;
use crate::native::{report_tests as f, *};

macro_rules! roundtrip {
    ($writer:path, $reader:path, $value:expr) => {{
        let value = $value;
        let mut count = CountingSink::new(usize::MAX, usize::MAX);
        $writer(&mut count, value).unwrap();
        let mut bytes = vec![0; count.len()];
        let mut output = SliceSink::new(&mut bytes, count.visits_used());
        $writer(&mut output, value).unwrap();
        output.finish().unwrap();
        let mut cursor = Cursor::new(&bytes, bytes.len(), usize::MAX).unwrap();
        assert_eq!($reader(&mut cursor).unwrap(), value);
        let visits = cursor.visits_used();
        cursor.finish().unwrap();
        let mut exact = Cursor::new(&bytes, bytes.len(), visits).unwrap();
        assert_eq!($reader(&mut exact).unwrap(), value);
        exact.finish().unwrap();
        let mut short = Cursor::new(&bytes, bytes.len(), visits - 1).unwrap();
        assert_eq!($reader(&mut short), Err(Error::Capacity));
        for end in 0..bytes.len() {
            let prefix = &bytes[..end];
            let mut cursor = Cursor::new(prefix, prefix.len(), usize::MAX).unwrap();
            assert_eq!($reader(&mut cursor), Err(Error::Truncated));
        }
        bytes
    }};
}

#[test]
fn exact_target_and_receipt_frames_preserve_all_recorded_coordinates() {
    let binding = Binding {
        revision: ObjectRevision(0),
        content: ContentHash([0; 32]),
        ..f::binding(1)
    };
    roundtrip!(types::binding, super::binding, binding);
    let receipt = ReceiptFence {
        receipt: ReceiptId::from_u128(22),
        epoch: u64::MAX,
    };
    roundtrip!(types::optional_receipt, optional_receipt, Some(receipt));
    roundtrip!(types::optional_receipt, optional_receipt, None);
    for target in [
        validation::Target::Admission { claim: binding },
        validation::Target::Increment {
            claim: binding,
            artifact: f::binding(2),
        },
        validation::Target::Artifact {
            response: f::binding(3),
            slot: u32::MAX,
            artifact: f::binding(2),
        },
        validation::Target::MissingSlot {
            response: f::binding(3),
            slot: 4,
        },
        validation::Target::Delivery {
            response: f::binding(3),
        },
    ] {
        roundtrip!(types::target, super::target, target);
    }
    roundtrip!(
        types::owner,
        owner,
        Some(creation::Owner {
            expected: binding,
            receipt: Some(receipt)
        })
    );
    roundtrip!(types::owner, owner, None);
    roundtrip!(
        types::scope_limits,
        scope_limits,
        scope::ScopeLimits {
            scopes: 0,
            roots: 17,
            children: 1
        }
    );
}

fn accepted_value() -> validation::AcceptedResultSnapshotV1 {
    validation::AcceptedResultSnapshotV1 {
        binding: f::binding(101),
        ledger: f::binding(1).ledger,
        claim: ClaimId::from_u128(1),
        target: validation::Target::Artifact {
            response: f::binding(300),
            slot: 7,
            artifact: f::binding(301),
        },
        validation: ValidationId::from_u128(101),
        declaration_index: 9,
        mode: ValidationMode::Observe,
        verdict: VerdictValue::Error,
        phase: validation::Phase::Quality,
        attempt: Some(13),
        generation: 17,
        receipt: Some(ReceiptFence {
            receipt: ReceiptId::from_u128(23),
            epoch: 29,
        }),
        evidence: Some(ArtifactRef {
            id: ArtifactId::from_u128(31),
            hash: ContentHash([37; 32]),
        }),
        programmatic_evidence: Some(ArtifactRef {
            id: ArtifactId::from_u128(41),
            hash: ContentHash([43; 32]),
        }),
        reporter: Some(ParticipantId::from_u128(47)),
        resulting_state: validation::State::ErroredNotRequired,
    }
}

#[test]
fn accepted_result_decodes_exact_history_without_inventing_attempt_or_proof() {
    roundtrip!(write::accepted_snapshot, accepted_result, accepted_value());
    let base = accepted_value();
    for phase in [
        validation::Phase::Delivery,
        validation::Phase::MissingTarget,
    ] {
        roundtrip!(
            write::accepted_snapshot,
            accepted_result,
            validation::AcceptedResultSnapshotV1 {
                phase,
                attempt: None,
                evidence: None,
                programmatic_evidence: None,
                reporter: None,
                ..base
            }
        );
    }
}

#[test]
fn complete_live_evaluation_snapshot_keeps_retry_and_quality_history() {
    let mut core = f::running(&[(ValidationMode::Required, true)]);
    let mut custody = f::Custody::new();
    for (request, verdict) in [(500, VerdictValue::Error), (501, VerdictValue::Pass)] {
        let input = f::report_for(
            &core,
            None,
            request,
            1,
            verdict,
            f::descriptor(f::artifact_spec(request + 1000, f::EVALUATOR, verdict)),
        );
        let evidence = f::verified(&mut custody, &input);
        let prepared = f::report(&core, input, &[], &evidence);
        core.publish_native(prepared).unwrap();
        let Some(Row::Evaluation(row)) = core.state.rows.get(&Key::Evaluation(f::key(1))) else {
            panic!("evaluation row");
        };
        let expected = row.get().unwrap().snapshot_v1().unwrap();
        let mut count = CountingSink::new(usize::MAX, usize::MAX);
        lifecycle::evaluation(&mut count, row).unwrap();
        let mut bytes = vec![0; count.len()];
        let mut output = SliceSink::new(&mut bytes, count.visits_used());
        lifecycle::evaluation(&mut output, row).unwrap();
        output.finish().unwrap();
        let mut cursor = Cursor::new(&bytes, bytes.len(), usize::MAX).unwrap();
        assert_eq!(evaluation_snapshot(&mut cursor).unwrap(), expected);
        let visits = cursor.visits_used();
        cursor.finish().unwrap();
        let mut short = Cursor::new(&bytes, bytes.len(), visits - 1).unwrap();
        assert_eq!(evaluation_snapshot(&mut short), Err(Error::Capacity));
    }
}

#[test]
fn terminal_provenance_and_authority_fences_preserve_every_optional_field() {
    let cause = aggregation::BlockingCauseSnapshotV1 {
        key: aggregation::CauseKey {
            target: aggregation::CauseTarget::Increment {
                artifact: ArtifactId::from_u128(7),
                content: ContentHash([11; 32]),
            },
            declaration_index: 13,
            generation: Some(17),
            attempt: Some(19),
            phase: aggregation::CausePhase::Quality,
        },
        slot: Some(23),
        artifact: Some(ArtifactRef {
            id: ArtifactId::from_u128(29),
            hash: ContentHash([31; 32]),
        }),
        kind: aggregation::BlockingKind::Errored,
        mode: ValidationMode::Required,
        slot_mode: ValidationMode::Observe,
        evidence: Some(ArtifactRef {
            id: ArtifactId::from_u128(37),
            hash: ContentHash([41; 32]),
        }),
    };
    roundtrip!(write::blocking_cause, blocking_cause, cause);
    roundtrip!(
        write::terminal,
        terminal,
        aggregation::TerminalCutSnapshotV1 {
            sequence: SessionSeq(43),
            cause
        }
    );
    let deadline = Deadline {
        timer: TimerId::from_u128(47),
        generation: 53,
        at: 59,
    };
    let origin = graph::OriginSnapshotV1 {
        binding: f::binding(61),
        created: SessionSeq(67),
        terminal: SessionSeq(71),
    };
    for kind in [
        graph::FailureKind::DependencyFailed,
        graph::FailureKind::Deadlocked,
    ] {
        roundtrip!(
            write::graph_terminal,
            graph_terminal,
            graph::TerminalCutSnapshotV1 {
                sequence: SessionSeq(73),
                kind,
                origin,
                fingerprint: ContentHash([79; 32]),
                deadline: Some(deadline),
                fired_at: Some(83),
            }
        );
    }
    for reason in [
        validation::FenceReason::Cancellation,
        validation::FenceReason::Revocation,
        validation::FenceReason::Supersession,
        validation::FenceReason::Expiry,
        validation::FenceReason::ReceiptAdoption,
        validation::FenceReason::Evaluation,
        validation::FenceReason::Deadline(deadline),
    ] {
        roundtrip!(
            write::optional_fence,
            optional_fence,
            Some(validation::AuthorityFence {
                reason,
                cause: ContentHash([89; 32])
            })
        );
    }
    roundtrip!(write::optional_fence, optional_fence, None);
}

#[test]
fn all_lifecycle_tags_roundtrip_and_unknown_tags_are_rejected() {
    for state in [
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
    ] {
        roundtrip!(write::evaluation_state, evaluation_state, state);
    }
    for state in [
        evidence::WorkArtifactState::Generated,
        evidence::WorkArtifactState::GenerationFailed,
        evidence::WorkArtifactState::Received,
        evidence::WorkArtifactState::ReceiptFailed,
        evidence::WorkArtifactState::Attached,
        evidence::WorkArtifactState::Validating,
        evidence::WorkArtifactState::Validated,
        evidence::WorkArtifactState::ValidationFailed,
    ] {
        roundtrip!(write::work_state, work_state, state);
    }
    for state in [
        evidence::ResponseState::Generated,
        evidence::ResponseState::Posted,
        evidence::ResponseState::Received,
        evidence::ResponseState::Validating,
        evidence::ResponseState::Validated,
        evidence::ResponseState::ValidationIncomplete,
        evidence::ResponseState::ValidationFailed,
        evidence::ResponseState::ValidationErrored,
    ] {
        roundtrip!(write::response_state, response_state, state);
    }
    for state in [
        ClaimStatus::Generated,
        ClaimStatus::Posted,
        ClaimStatus::Received,
        ClaimStatus::Progressed,
        ClaimStatus::TestamentGenerated,
        ClaimStatus::TestamentAcknowledged,
        ClaimStatus::Validating,
        ClaimStatus::Satisfied,
        ClaimStatus::PostFailed,
        ClaimStatus::ReceiptFailed,
        ClaimStatus::TestamentGenerationFailed,
        ClaimStatus::ValidationIncomplete,
        ClaimStatus::ValidationFailed,
        ClaimStatus::ValidationErrored,
        ClaimStatus::Cancelled,
        ClaimStatus::Expired,
        ClaimStatus::Revoked,
        ClaimStatus::Superseded,
        ClaimStatus::DependencyFailed,
        ClaimStatus::Deadlocked,
    ] {
        roundtrip!(write::claim_status, claim_status, state);
    }
    let invalid = [255; 4];
    macro_rules! rejects {
        ($reader:path) => {{
            let mut cursor = Cursor::new(&invalid, invalid.len(), 32).unwrap();
            assert!(matches!($reader(&mut cursor), Err(Error::InvalidTag(_))));
        }};
    }
    rejects!(boolean);
    rejects!(optional_binding);
    rejects!(mode);
    rejects!(verdict);
    rejects!(failure);
    rejects!(phase);
    rejects!(target);
    rejects!(claim_status);
    rejects!(work_state);
    rejects!(response_state);
    rejects!(result_testament_state);
    rejects!(evaluation_state);
    rejects!(optional_suppression);
    rejects!(optional_fence);
    rejects!(claim_terminal);
    rejects!(work_terminal);
    rejects!(response_terminal);
}
