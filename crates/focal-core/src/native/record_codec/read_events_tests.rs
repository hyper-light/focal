use super::super::{
    bytes::{CountingSink, SliceSink},
    events,
};
use super::*;
use crate::native::{report_tests as f, *};
use focal_model::lifecycle::{
    audit,
    claim::{ClaimCut, ReceiptEntitlement},
    evidence, validation,
};
use focal_model::{ContentHash, ObjectRevision, ReceiptFence, ReceiptId, SessionSeq};

fn encoded(value: NativeEvent) -> Vec<u8> {
    let mut count = CountingSink::new(usize::MAX, usize::MAX);
    events::event(&mut count, value).unwrap();
    let mut bytes = vec![0; count.len()];
    let mut output = SliceSink::new(&mut bytes, count.visits_used());
    events::event(&mut output, value).unwrap();
    output.finish().unwrap();
    bytes
}
fn assert_roundtrip(value: NativeEvent) {
    let bytes = encoded(value);
    let mut cursor = Cursor::new(&bytes, bytes.len(), usize::MAX).unwrap();
    assert_eq!(event(&mut cursor).unwrap(), value);
    let visits = cursor.visits_used();
    cursor.finish().unwrap();
    let mut exact = Cursor::new(&bytes, bytes.len(), visits).unwrap();
    assert_eq!(event(&mut exact).unwrap(), value);
    exact.finish().unwrap();
    let mut short = Cursor::new(&bytes, bytes.len(), visits - 1).unwrap();
    assert_eq!(event(&mut short), Err(Error::Capacity));
}

#[test]
fn graph_capture_preserves_exact_exclusive_boundary_and_refuses_impossible_provenance() {
    let value = frame(NativeFact::Claim(NativeClaimEvent {
        kind: NativeEventKind::DependencyFailed,
        graph: Some(NativeGraphCapture { before_ordinal: 17 }),
        owned_child: None,
        before: Some(f::binding(1)),
        after: f::binding(1).next().unwrap(),
        status: ClaimStatus::DependencyFailed,
    }));
    assert_roundtrip(value);
    let bytes = encoded(value);
    // Independent literal suffix: Some tag, then the exclusive u32 LE cut.
    assert_eq!(&bytes[bytes.len() - 5..], &[1, 17, 0, 0, 0]);
    let packed = crate::native::history::StoredEvent::pack(value).unwrap();
    assert_eq!(packed.expand(f::ledger()), value);
    let NativeFact::Claim(mut claim) = value.fact else {
        panic!("claim");
    };
    for graph in [None, Some(NativeGraphCapture { before_ordinal: 24 })] {
        claim.graph = graph;
        let invalid = NativeEvent {
            fact: NativeFact::Claim(claim),
            ..value
        };
        assert!(crate::native::history::StoredEvent::pack(invalid).is_err());
        let mut sink = CountingSink::new(usize::MAX, usize::MAX);
        assert!(events::event(&mut sink, invalid).is_err());
    }
    claim.graph = Some(NativeGraphCapture { before_ordinal: 0 });
    claim.kind = NativeEventKind::Posted;
    assert!(
        crate::native::history::StoredEvent::pack(NativeEvent {
            fact: NativeFact::Claim(claim),
            ..value
        })
        .is_err()
    );
    let mut invalid = bytes;
    let tag = invalid.len() - 5;
    invalid[tag] = 2;
    let mut cursor = Cursor::new(&invalid, invalid.len(), usize::MAX).unwrap();
    assert!(matches!(event(&mut cursor), Err(Error::InvalidTag(_))));
}
fn result_key() -> NativeResultKey {
    NativeResultKey {
        evaluation: f::key(1),
        revision: ObjectRevision(13),
    }
}
fn frame(fact: NativeFact) -> NativeEvent {
    NativeEvent {
        invocation: NativeInvocation::Request(f::request(f::ISSUER, 17)),
        sequence: SessionSeq(19),
        ordinal: 23,
        fact,
    }
}
fn receipt() -> ReceiptFence {
    ReceiptFence {
        receipt: ReceiptId::from_u128(29),
        epoch: 31,
    }
}
fn entitlement() -> ReceiptEntitlement {
    ReceiptEntitlement {
        holder: f::SUBJECT,
        fence: receipt(),
    }
}
fn attempt() -> validation::Attempt {
    validation::Attempt {
        phase: validation::Phase::Quality,
        index: 37,
        handler: focal_model::ValidatorId::from_u128(41),
        version: ContentHash([43; 32]),
        evaluator: f::QUALITY,
        definition: ContentHash([47; 32]),
    }
}
fn cut() -> ClaimCut {
    ClaimCut {
        position: SessionSeq(53),
        cause: ContentHash([59; 32]),
    }
}

#[test]
fn every_native_fact_preserves_full_recorded_fields_and_budget() {
    let facts = [
        NativeFact::ResultTestament {
            claim: ClaimId::from_u128(1),
            before: Some(f::binding(2)),
            after: f::binding(3),
            state: audit::ResultTestamentState::Posted,
        },
        NativeFact::Missing { key: result_key() },
        NativeFact::Registrations {
            claim: f::binding(1),
        },
        NativeFact::Delivery { key: result_key() },
        NativeFact::Work {
            claim: ClaimId::from_u128(1),
            before: None,
            after: f::binding(4),
            state: evidence::WorkArtifactState::ReceiptFailed,
        },
        NativeFact::Diagnostic {
            claim: ClaimId::from_u128(1),
            binding: f::binding(5),
            reason: evidence::EvidenceFailure::Production,
        },
        NativeFact::Response {
            claim: ClaimId::from_u128(1),
            before: Some(f::binding(6)),
            after: f::binding(7),
            state: evidence::ResponseState::ValidationErrored,
        },
        NativeFact::Receipt {
            claim: f::binding(1),
            fence: receipt(),
            holder: f::SUBJECT,
        },
        NativeFact::ReceiptAdopted {
            claim: f::binding(1),
            previous: entitlement(),
            replacement: ReceiptEntitlement {
                holder: f::EVALUATOR,
                fence: ReceiptFence {
                    epoch: 61,
                    ..receipt()
                },
            },
            cause: ContentHash([67; 32]),
        },
        NativeFact::Artifact {
            binding: f::binding(8),
        },
        NativeFact::Accepted { key: result_key() },
        NativeFact::Claim(NativeClaimEvent {
            graph: None,
            kind: NativeEventKind::Superseded,
            owned_child: Some(f::binding(9)),
            before: Some(f::binding(10)),
            after: f::binding(11),
            status: ClaimStatus::Superseded,
        }),
        NativeFact::Definition {
            binding: f::binding(12),
            claim: ClaimId::from_u128(1),
            index: 71,
            intent: ContentHash([73; 32]),
        },
        NativeFact::Evaluation {
            kind: NativeEvaluationEventKind::Reported,
            key: f::key(1),
            before: Some(f::binding(13)),
            after: f::binding(14),
            state: validation::State::ErroredNotRequired,
            phase: validation::Phase::Quality,
            attempt: Some(attempt()),
            fence: Some(validation::AuthorityFence {
                reason: validation::FenceReason::Deadline(Deadline {
                    timer: TimerId::from_u128(79),
                    generation: 83,
                    at: 89,
                }),
                cause: ContentHash([97; 32]),
            }),
        },
    ];
    for fact in facts {
        assert_roundtrip(frame(fact));
    }
}

#[test]
fn claim_monitor_evaluation_and_invocation_tags_all_roundtrip() {
    let kinds = [
        NativeEventKind::OwnerReleased,
        NativeEventKind::Validating,
        NativeEventKind::LocallyComplete,
        NativeEventKind::ValidationIncomplete,
        NativeEventKind::ValidationFailed,
        NativeEventKind::ValidationErrored,
        NativeEventKind::DependencyFailed,
        NativeEventKind::Created,
        NativeEventKind::ChildRegistered,
        NativeEventKind::Superseded,
        NativeEventKind::Cancelled,
        NativeEventKind::Posted,
        NativeEventKind::PostFailed,
        NativeEventKind::Received,
        NativeEventKind::ReceiptAdopted,
        NativeEventKind::Satisfied,
        NativeEventKind::TestamentGenerated,
        NativeEventKind::TestamentAcknowledged,
        NativeEventKind::ResponseObserved,
        NativeEventKind::Expired,
        NativeEventKind::Deadlocked,
    ];
    let make = |kind| {
        frame(NativeFact::Claim(NativeClaimEvent {
            graph: matches!(
                kind,
                NativeEventKind::DependencyFailed
                    | NativeEventKind::Deadlocked
                    | NativeEventKind::Satisfied
                    | NativeEventKind::Expired
            )
            .then_some(NativeGraphCapture { before_ordinal: 17 }),
            kind,
            owned_child: None,
            before: None,
            after: f::binding(1),
            status: ClaimStatus::Generated,
        }))
    };
    for kind in kinds {
        assert_roundtrip(make(kind));
    }
    for monitor in [
        NativeMonitorEvent::Registered {
            id: MonitorId::from_u128(1),
            cut: cut(),
        },
        NativeMonitorEvent::Rebound {
            id: MonitorId::from_u128(2),
            change: scope::Rebinding {
                predecessor: ClaimId::from_u128(3),
                successor: ClaimId::from_u128(4),
                cut: cut(),
            },
        },
        NativeMonitorEvent::Released {
            id: MonitorId::from_u128(5),
            cut: cut(),
        },
        NativeMonitorEvent::Cancelled {
            id: MonitorId::from_u128(6),
            cancellation: scope::MonitorCancellation {
                terminal: SessionSeq(7),
                cut: cut(),
            },
        },
    ] {
        assert_roundtrip(make(NativeEventKind::Monitor(monitor)));
    }
    for kind in [
        NativeEvaluationEventKind::MissingTarget,
        NativeEvaluationEventKind::Materialized,
        NativeEvaluationEventKind::Begun,
        NativeEvaluationEventKind::Reported,
        NativeEvaluationEventKind::AuthorityFenced,
        NativeEvaluationEventKind::Sealed,
    ] {
        assert_roundtrip(frame(NativeFact::Evaluation {
            kind,
            key: f::key(1),
            before: None,
            after: f::binding(2),
            state: validation::State::Ready,
            phase: validation::Phase::Programmatic,
            attempt: None,
            fence: None,
        }));
    }
    for invocation in [
        NativeInvocation::Request(f::request(f::SUBJECT, 13)),
        NativeInvocation::EvaluationDeadline(NativeDeadlineKey {
            evaluation: f::key(1),
            timer: TimerId::from_u128(17),
            generation: 19,
        }),
        NativeInvocation::ClaimDeadline(NativeClaimDeadlineKey {
            claim: ClaimId::from_u128(23),
            timer: TimerId::from_u128(29),
            generation: 31,
        }),
        NativeInvocation::MonitorDeadline(NativeMonitorDeadlineKey {
            claim: ClaimId::from_u128(37),
            monitor: MonitorId::from_u128(41),
            timer: TimerId::from_u128(43),
            generation: 47,
        }),
    ] {
        assert_roundtrip(NativeEvent {
            invocation,
            ..frame(NativeFact::Artifact {
                binding: f::binding(53),
            })
        });
    }
}

#[test]
fn actual_published_events_roundtrip_and_parser_preserves_its_outer_boundary() {
    let mut core = f::core();
    let outcome = f::publish(
        &mut core,
        1,
        f::creation(
            1,
            1,
            &[(focal_model::ValidationMode::Required, false)],
            None,
        ),
    );
    for ordinal in 0..outcome.events {
        assert_roundtrip(core.native_event(outcome.sequence, ordinal).unwrap());
    }
    let value = frame(NativeFact::Evaluation {
        kind: NativeEvaluationEventKind::Reported,
        key: f::key(1),
        before: Some(f::binding(2)),
        after: f::binding(3),
        state: validation::State::Validated,
        phase: validation::Phase::Quality,
        attempt: Some(attempt()),
        fence: Some(validation::AuthorityFence {
            reason: validation::FenceReason::ReceiptAdoption,
            cause: ContentHash([5; 32]),
        }),
    });
    let bytes = encoded(value);
    for end in 0..bytes.len() {
        let truncated = &bytes[..end];
        let mut cursor = Cursor::new(truncated, truncated.len(), usize::MAX).unwrap();
        assert_eq!(event(&mut cursor), Err(Error::Truncated));
    }
    let mut suffixed = bytes.clone();
    suffixed.push(0xff);
    let mut cursor = Cursor::new(&suffixed, suffixed.len(), usize::MAX).unwrap();
    assert_eq!(event(&mut cursor).unwrap(), value);
    assert_eq!(cursor.finish(), Err(Error::TrailingBytes));
    let mut invalid = bytes;
    invalid[0] = 0xff;
    let mut cursor = Cursor::new(&invalid, invalid.len(), usize::MAX).unwrap();
    assert!(matches!(event(&mut cursor), Err(Error::InvalidTag(_))));
    macro_rules! reject_tag {
        ($reader:path) => {{
            let mut cursor = Cursor::new(&[0xff], 1, 2).unwrap();
            assert!(matches!($reader(&mut cursor), Err(Error::InvalidTag(_))));
        }};
    }
    reject_tag!(claim_kind);
    reject_tag!(evaluation_kind);
    reject_tag!(monitor);
}
