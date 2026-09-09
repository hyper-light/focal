use super::*;
use crate::native::{NativeClaimDeadlineKey, NativeGraphCapture};
use focal_model::{
    ClaimStatus, ContentHash, ObjectId, ObjectRevision, RequestEpoch, RequestId, RequestKey,
    TimerId, ValidationId,
};

fn ledger() -> LedgerId {
    LedgerId {
        tenant: focal_model::TenantId::from_u128(3),
        session: focal_model::SessionId::from_u128(4),
    }
}
fn binding(id: u128, revision: u64) -> Binding {
    Binding {
        ledger: ledger(),
        object: ObjectId::from_u128(id),
        content: ContentHash([9; 32]),
        revision: ObjectRevision(revision),
    }
}
fn request() -> NativeInvocation {
    NativeInvocation::Request(RequestKey {
        principal: ParticipantId::from_u128(77),
        epoch: RequestEpoch(1),
        id: RequestId::from_u128(5),
    })
}
fn event(invocation: NativeInvocation, ordinal: u32, fact: NativeFact) -> NativeEvent {
    NativeEvent {
        invocation,
        sequence: SessionSeq(9),
        ordinal,
        fact,
    }
}
fn claim_fact(kind: NativeEventKind, status: ClaimStatus) -> NativeFact {
    NativeFact::Claim(NativeClaimEvent {
        kind,
        graph: matches!(kind, NativeEventKind::Satisfied)
            .then_some(NativeGraphCapture { before_ordinal: 0 }),
        owned_child: None,
        before: Some(binding(21, 1)),
        after: binding(21, 2),
        status,
    })
}

#[test]
fn deltas_carry_the_record_the_nearest_action_the_actor_and_the_claim() {
    let created = event(
        request(),
        0,
        claim_fact(NativeEventKind::Created, ClaimStatus::Generated),
    );
    let delta = super::delta(ledger(), SessionSeq(41), created);
    assert_eq!(delta.schema, NATIVE_DELTA_SCHEMA);
    assert_eq!(
        delta.id,
        DeltaId {
            ledger: ledger(),
            sequence: SessionSeq(41),
            ordinal: 0
        }
    );
    assert_eq!(delta.action, LifecycleAction::Generated);
    assert_eq!(delta.actor, ParticipantId::from_u128(77));
    assert_eq!(delta.claim, Some(ClaimId::from_u128(21)));
    let DeltaFact::Native(record) = &delta.fact else {
        panic!("native fact");
    };
    // The record keeps the native position even when the stream line differs.
    assert_eq!(record.sequence, SessionSeq(9));
    assert_eq!(record.ordinal, 0);
    assert_eq!(**record, event_record(created));
    assert!(matches!(
        record.fact,
        NativeFactRecord::Claim(NativeClaimEventRecord {
            kind: NativeEventKindRecord::Created,
            status: ClaimStatus::Generated,
            before: Some(_),
            ..
        })
    ));

    // Trusted timers and the import are attributed to no participant.
    let expired = event(
        NativeInvocation::ClaimDeadline(NativeClaimDeadlineKey {
            claim: ClaimId::from_u128(21),
            timer: TimerId::from_u128(2),
            generation: 1,
        }),
        1,
        claim_fact(NativeEventKind::Expired, ClaimStatus::Expired),
    );
    let delta = super::delta(ledger(), SessionSeq(42), expired);
    assert_eq!(delta.actor, ParticipantId::default());
    assert_eq!(delta.action, LifecycleAction::Expired);
    assert_eq!(delta.id.ordinal, 1);
    let imported = event(
        NativeInvocation::Import,
        3,
        claim_fact(
            NativeEventKind::Imported(SessionSeq(12)),
            ClaimStatus::Satisfied,
        ),
    );
    assert_eq!(delta_actor(imported.invocation), ParticipantId::default());
    // An imported status fact maps exactly like the legacy status did.
    assert_eq!(delta_action(&imported.fact), LifecycleAction::Satisfied);

    // A registered artifact belongs to no claim; work does.
    let artifact = NativeFact::Artifact {
        binding: binding(55, 1),
    };
    assert_eq!(delta_claim(&artifact), None);
    assert_eq!(delta_action(&artifact), LifecycleAction::ArtifactAttached);
    let work = NativeFact::Work {
        claim: ClaimId::from_u128(21),
        before: None,
        after: binding(56, 1),
        state: evidence::WorkArtifactState::ReceiptFailed,
    };
    assert_eq!(delta_claim(&work), Some(ClaimId::from_u128(21)));
    assert_eq!(delta_action(&work), LifecycleAction::ReceiptFailed);

    // Evaluations map through their state; keys name the claim.
    let key = EvaluationKey {
        claim: ClaimId::from_u128(21),
        validation: ValidationId::from_u128(30),
        target: EvaluationTarget::Admission,
        generation: 2,
    };
    let evaluation = |state: validation::State| NativeFact::Evaluation {
        kind: super::EventKind::Reported,
        key,
        before: Some(binding(30, 1)),
        after: binding(30, 2),
        state,
        phase: validation::Phase::Programmatic,
        attempt: None,
        fence: None,
    };
    assert_eq!(
        delta_action(&evaluation(validation::State::Validated)),
        LifecycleAction::ValidationVerdict
    );
    assert_eq!(
        delta_action(&evaluation(validation::State::Errored)),
        LifecycleAction::ValidationErrored
    );
    assert_eq!(
        delta_action(&evaluation(validation::State::Ready)),
        LifecycleAction::ValidationScheduled
    );
    assert_eq!(
        delta_claim(&evaluation(validation::State::Ready)),
        Some(ClaimId::from_u128(21))
    );
    let accepted = NativeFact::Accepted {
        key: NativeResultKey {
            evaluation: key,
            revision: ObjectRevision(3),
        },
    };
    assert_eq!(delta_claim(&accepted), Some(ClaimId::from_u128(21)));
    assert_eq!(delta_action(&accepted), LifecycleAction::ValidationVerdict);
    // Monitor facts are scope actions of the owner claim.
    let released = claim_fact(
        NativeEventKind::Monitor(NativeMonitorEvent::Released {
            id: focal_model::MonitorId::from_u128(8),
            cut: claim::ClaimCut {
                position: SessionSeq(9),
                cause: ContentHash([1; 32]),
            },
        }),
        ClaimStatus::Satisfied,
    );
    assert_eq!(delta_action(&released), LifecycleAction::ScopeReleased);
    assert_eq!(delta_claim(&released), Some(ClaimId::from_u128(21)));
}
