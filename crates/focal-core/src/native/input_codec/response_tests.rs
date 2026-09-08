use super::*;
use focal_model::{ObjectId, ObjectRevision, RequestEpoch, RequestId, SessionId, TenantId};

const LEDGER: LedgerId = LedgerId {
    tenant: TenantId::from_u128(61),
    session: SessionId::from_u128(62),
};
fn binding(id: u128) -> Binding {
    Binding {
        ledger: LEDGER,
        object: ObjectId::from_u128(id),
        content: ContentHash([7; 32]),
        revision: ObjectRevision(8),
    }
}
fn request() -> RequestKey {
    RequestKey {
        principal: ParticipantId::from_u128(63),
        epoch: RequestEpoch(3),
        id: RequestId::from_u128(64),
    }
}
fn artifact(id: u128) -> ArtifactRef {
    ArtifactRef {
        id: ArtifactId::from_u128(id),
        hash: ContentHash([9; 32]),
    }
}
fn response(confidence: Confidence, outcome: OutcomeKind) -> NativeInput {
    NativeInput {
        request: request(),
        command: NativeCommand::CloseResponse {
            claim: binding(65),
            response: binding(66),
            report: NativeResponseInput {
                summary: "Retained failure: é\n".into(),
                confidence,
                outcome,
                manifest: vec![
                    SlotBinding {
                        slot: 9,
                        artifact: artifact(67),
                    },
                    SlotBinding {
                        slot: 3,
                        artifact: artifact(68),
                    },
                ],
                diagnostics: vec![artifact(70), artifact(69), artifact(70)],
            },
        },
    }
}
fn monitor(receipt: bool) -> NativeInput {
    NativeInput {
        request: request(),
        command: NativeCommand::RegisterMonitor {
            expected: binding(65),
            receipt: receipt.then_some(ReceiptFence {
                receipt: ReceiptId::from_u128(71),
                epoch: 72,
            }),
            id: MonitorId::from_u128(73),
            roots: vec![
                WaitPredicate::Released(ClaimId::from_u128(74)),
                WaitPredicate::Satisfied(ClaimId::from_u128(75)),
                WaitPredicate::Terminal(ClaimId::from_u128(76)),
                WaitPredicate::Released(ClaimId::from_u128(74)),
            ],
            deadline: Deadline {
                timer: TimerId::from_u128(77),
                generation: 78,
                at: 79,
            },
        },
    }
}
fn encoded(input: &NativeInput) -> Vec<u8> {
    let plan = EncodingPlan::prepare(
        InputFrame::Request {
            ledger: LEDGER,
            profile: NativeContentProfile::ProjectionOnly,
            input,
        },
        EncodingLimits {
            bytes: 1 << 20,
            visits: 1 << 20,
        },
    )
    .unwrap();
    let mut bytes = vec![0; plan.quote().bytes];
    plan.write_into(&mut bytes).unwrap();
    bytes
}
fn inspected(bytes: &[u8]) -> StructuralInput<'_> {
    StructuralInput::inspect(
        bytes,
        InspectionLimits {
            bytes: 1 << 20,
            visits: 1 << 20,
            items: 4096,
            text_bytes: 4096,
            blob_bytes: 4096,
        },
    )
    .unwrap()
}
fn capacity<T: std::fmt::Debug>(result: Result<T, DecodeError>) {
    assert!(
        matches!(
            result,
            Err(DecodeError::Native(NativeError::Capacity(_)))
                | Err(DecodeError::Codec(CodecError::Capacity))
        ),
        "{result:?}"
    );
}

#[test]
fn borrowed_response_decoding_preserves_every_outcome_and_native_request_identity() {
    for confidence in [
        Confidence::Hint,
        Confidence::Tentative,
        Confidence::Committed,
        Confidence::Consensus,
    ] {
        for outcome in [
            OutcomeKind::Complete,
            OutcomeKind::Partial,
            OutcomeKind::Refused,
            OutcomeKind::Impossible,
            OutcomeKind::Interrupted,
            OutcomeKind::Failed,
        ] {
            let input = response(confidence, outcome);
            let original = super::super::super::intent::fingerprint(LEDGER, &input).unwrap();
            let bytes = encoded(&input);
            let inspected = inspected(&bytes);
            let plan = inspected
                .prepare_response(NativeLimits::default(), 1 << 20)
                .unwrap()
                .unwrap();
            assert_eq!(plan.header(), inspected.header());
            assert_eq!(plan.claim(), binding(65));
            assert_eq!(plan.response(), binding(66));
            assert_eq!(plan.intent(), original);
            assert_eq!(plan.summary().as_ptr(), bytes[85 + 2 * 88 + 4..].as_ptr());
            let quote = plan.quote();
            assert_eq!(quote.allocations, 3);
            assert_eq!(
                inspected
                    .prepare_response(NativeLimits::default(), quote.prepare_visits)
                    .unwrap()
                    .unwrap()
                    .quote(),
                quote
            );
            let built = plan.build(quote.bytes, quote.build_visits).unwrap();
            assert_eq!(
                super::super::super::intent::fingerprint(LEDGER, &built).unwrap(),
                original
            );
            assert_eq!(encoded(&built), bytes);
            let NativeCommand::CloseResponse { report, .. } = built.command else {
                panic!("response");
            };
            assert_eq!(report.confidence, confidence);
            assert_eq!(report.outcome, outcome);
            assert_eq!(report.heap_charge().unwrap(), quote.bytes);
            assert_eq!(
                report.diagnostics,
                [artifact(70), artifact(69), artifact(70)]
            );
        }
    }
}

#[test]
fn response_dimensions_bytes_and_all_stages_of_work_refuse_at_one_short() {
    let input = response(Confidence::Committed, OutcomeKind::Failed);
    let bytes = encoded(&input);
    let inspected = inspected(&bytes);
    let plan = inspected
        .prepare_response(NativeLimits::default(), 1 << 20)
        .unwrap()
        .unwrap();
    let quote = plan.quote();
    capacity(inspected.prepare_response(NativeLimits::default(), quote.prepare_visits - 1));
    capacity(plan.build(quote.bytes - 1, quote.build_visits));
    let plan = inspected
        .prepare_response(NativeLimits::default(), quote.prepare_visits)
        .unwrap()
        .unwrap();
    capacity(plan.build(quote.bytes, quote.build_visits - 1));
    for limits in [
        NativeLimits {
            response_summary_bytes: 1,
            ..NativeLimits::default()
        },
        NativeLimits {
            work_artifacts_per_cycle: 1,
            ..NativeLimits::default()
        },
        NativeLimits {
            diagnostics_per_cycle: 2,
            ..NativeLimits::default()
        },
        NativeLimits {
            preparation_bytes: quote.bytes - 1,
            ..NativeLimits::default()
        },
    ] {
        capacity(inspected.prepare_response(limits, 1 << 20));
    }
    let empty = NativeInput {
        request: request(),
        command: NativeCommand::CloseResponse {
            claim: binding(65),
            response: binding(66),
            report: NativeResponseInput {
                summary: String::new(),
                confidence: Confidence::Hint,
                outcome: OutcomeKind::Failed,
                manifest: Vec::new(),
                diagnostics: Vec::new(),
            },
        },
    };
    let bytes = encoded(&empty);
    let plan = inspected_empty(&bytes);
    let quote = plan.quote();
    assert_eq!((quote.bytes, quote.allocations), (0, 0));
    let built = plan.build(0, quote.build_visits).unwrap();
    assert_eq!(encoded(&built), bytes);
}
fn inspected_empty(bytes: &[u8]) -> ResponseFramePlan<'_> {
    inspected(bytes)
        .prepare_response(NativeLimits::default(), 1 << 20)
        .unwrap()
        .unwrap()
}

#[test]
fn borrowed_monitor_roots_preserve_all_predicates_order_fences_and_original_intent() {
    for receipt in [false, true] {
        let input = monitor(receipt);
        let original = super::super::super::intent::fingerprint(LEDGER, &input).unwrap();
        let bytes = encoded(&input);
        let inspected = inspected(&bytes);
        let plan = inspected
            .prepare_monitor(NativeLimits::default(), 1 << 20)
            .unwrap()
            .unwrap();
        assert_eq!(plan.header(), inspected.header());
        assert_eq!(plan.expected(), binding(65));
        assert_eq!(plan.id(), MonitorId::from_u128(73));
        assert_eq!(plan.receipt().is_some(), receipt);
        assert_eq!(
            plan.deadline(),
            Deadline {
                timer: TimerId::from_u128(77),
                generation: 78,
                at: 79
            }
        );
        assert_eq!(plan.intent(), original);
        let quote = plan.quote();
        assert_eq!(quote.allocations, 1);
        assert_eq!(
            inspected
                .prepare_monitor(NativeLimits::default(), quote.prepare_visits)
                .unwrap()
                .unwrap()
                .quote(),
            quote
        );
        capacity(inspected.prepare_monitor(NativeLimits::default(), quote.prepare_visits - 1));
        let built = plan.build(quote.bytes, quote.build_visits).unwrap();
        assert_eq!(
            super::super::super::intent::fingerprint(LEDGER, &built).unwrap(),
            original
        );
        assert_eq!(encoded(&built), bytes);
        capacity(inspected.prepare_monitor(
            NativeLimits {
                plan_edges: 3,
                ..NativeLimits::default()
            },
            1 << 20,
        ));
        let plan = inspected
            .prepare_monitor(NativeLimits::default(), quote.prepare_visits)
            .unwrap()
            .unwrap();
        capacity(plan.build(quote.bytes - 1, quote.build_visits));
        let plan = inspected
            .prepare_monitor(NativeLimits::default(), quote.prepare_visits)
            .unwrap()
            .unwrap();
        capacity(plan.build(quote.bytes, quote.build_visits - 1));
    }
}

#[test]
fn typed_passes_do_not_claim_unrelated_families_or_semantic_admission() {
    let input = NativeInput {
        request: request(),
        command: NativeCommand::Cancel {
            expected: binding(65),
        },
    };
    let bytes = encoded(&input);
    let inspection = inspected(&bytes);
    assert!(
        inspection
            .prepare_response(NativeLimits::default(), 0)
            .unwrap()
            .is_none()
    );
    assert!(
        inspection
            .prepare_monitor(NativeLimits::default(), 0)
            .unwrap()
            .is_none()
    );
    let mut input = monitor(false);
    let NativeCommand::RegisterMonitor { roots, .. } = &mut input.command else {
        panic!("monitor");
    };
    roots.clear();
    // This is representable input, not a claim that an empty monitor is valid.
    let bytes = encoded(&input);
    let inspection = inspected(&bytes);
    let plan = inspection
        .prepare_monitor(NativeLimits::default(), 1 << 20)
        .unwrap()
        .unwrap();
    let quote = plan.quote();
    assert_eq!((quote.bytes, quote.allocations), (0, 0));
    let built = plan.build(0, quote.build_visits).unwrap();
    assert_eq!(encoded(&built), bytes);
}

#[test]
fn changed_bytes_change_complete_identity_and_invalid_closed_tags_never_reach_plans() {
    let first = response(Confidence::Committed, OutcomeKind::Failed);
    let second = response(Confidence::Consensus, OutcomeKind::Partial);
    let one = encoded(&first);
    let two = encoded(&second);
    let one = inspected(&one)
        .prepare_response(NativeLimits::default(), 1 << 20)
        .unwrap()
        .unwrap()
        .intent();
    let two = inspected(&two)
        .prepare_response(NativeLimits::default(), 1 << 20)
        .unwrap()
        .unwrap()
        .intent();
    assert_ne!(one, two);
    let mut bytes = encoded(&monitor(false));
    // RegisterMonitor header, expected binding, absent receipt, monitor ID,
    // count, then the first closed root tag.
    bytes[85 + 88 + 1 + 16 + 4] = 3;
    assert!(matches!(
        StructuralInput::inspect(
            &bytes,
            InspectionLimits {
                bytes: 1 << 20,
                visits: 1 << 20,
                items: 4096,
                text_bytes: 4096,
                blob_bytes: 4096
            }
        ),
        Err(CodecError::InvalidTag("wait predicate"))
    ));
}
