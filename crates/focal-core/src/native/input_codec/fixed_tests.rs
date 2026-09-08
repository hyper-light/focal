use super::*;
use crate::native::intent;
use focal_model::lifecycle::evidence::SlotBinding;
use focal_model::{
    ArtifactRef, Confidence, ObjectId, ObjectRevision, OutcomeKind, RequestEpoch, RequestId,
    SessionId, TenantId,
};

const LEDGER: LedgerId = LedgerId {
    tenant: TenantId::from_u128(1),
    session: SessionId::from_u128(2),
};
const ACTOR: ParticipantId = ParticipantId::from_u128(3);

fn binding(id: u128) -> Binding {
    Binding {
        ledger: LEDGER,
        object: ObjectId::from_u128(id),
        content: ContentHash([41; 32]),
        revision: ObjectRevision(0x0102030405060708),
    }
}

fn request(command: NativeCommand) -> NativeInput {
    NativeInput {
        request: RequestKey {
            principal: ACTOR,
            epoch: RequestEpoch(0x0807060504030201),
            id: RequestId::from_u128(4),
        },
        command,
    }
}

fn deadline() -> Deadline {
    Deadline {
        timer: TimerId::from_u128(5),
        generation: 0x0102030405060708,
        at: 0x0807060504030201,
    }
}

fn receipt() -> ReceiptFence {
    ReceiptFence {
        receipt: ReceiptId::from_u128(6),
        epoch: 0x0102030405060708,
    }
}

fn evaluation(target: EvaluationTarget) -> EvaluationKey {
    EvaluationKey {
        claim: ClaimId::from_u128(10),
        validation: ValidationId::from_u128(11),
        target,
        generation: 0x0807060504030201,
    }
}

fn work_target() -> EvaluationTarget {
    EvaluationTarget::Work {
        response: TestamentId::from_u128(12),
        slot: 0x01020304,
        artifact: ArtifactId::from_u128(13),
    }
}

fn frame(input: &NativeInput) -> InputFrame<'_> {
    InputFrame::Request {
        ledger: LEDGER,
        profile: NativeContentProfile::ProjectionOnly,
        input,
    }
}

fn encode(source: InputFrame<'_>) -> Vec<u8> {
    let plan = EncodingPlan::prepare(
        source,
        EncodingLimits {
            bytes: 8192,
            visits: 32768,
        },
    )
    .unwrap();
    let mut output = vec![0; plan.quote().bytes];
    plan.write_into(&mut output).unwrap();
    output
}

fn inspect(bytes: &[u8]) -> StructuralInput<'_> {
    StructuralInput::inspect(
        bytes,
        InspectionLimits {
            bytes: 8192,
            visits: 32768,
            items: 32,
            text_bytes: 128,
            blob_bytes: 128,
        },
    )
    .unwrap()
}

fn native_intent(source: InputFrame<'_>) -> Result<ContentHash, NativeError> {
    match source {
        InputFrame::Request { ledger, input, .. } => intent::fingerprint(ledger, input),
        InputFrame::EvaluationDeadline { ledger, input, .. } => {
            intent::deadline_fingerprint(ledger, input)
        }
        InputFrame::ClaimDeadline { ledger, input, .. } => {
            intent::claim_deadline_fingerprint(ledger, input)
        }
        InputFrame::MonitorDeadline { ledger, input, .. } => {
            intent::monitor_deadline_fingerprint(ledger, input)
        }
    }
}

fn roundtrip(source: InputFrame<'_>) {
    let expected_intent = native_intent(source).unwrap();
    let bytes = encode(source);
    let inspected = inspect(&bytes);
    let visits = inspected.quote().visits;
    assert!(matches!(
        inspected.decode_fixed(visits - 1),
        Err(CodecError::Capacity)
    ));
    let decoded = inspected.decode_fixed(visits).unwrap().unwrap();
    assert_eq!(encode(decoded.as_frame()), bytes);
    assert_eq!(native_intent(decoded.as_frame()).unwrap(), expected_intent);
}

fn fixed_commands() -> Vec<(u8, NativeCommand)> {
    let claim = binding(10);
    let expected = binding(11);
    let admission = evaluation(EvaluationTarget::Admission);
    let increment = evaluation(EvaluationTarget::Increment {
        artifact: ArtifactId::from_u128(13),
    });
    vec![
        (1, NativeCommand::Cancel { expected }),
        (2, NativeCommand::Post { expected }),
        (
            3,
            NativeCommand::BeginAdmission {
                claim,
                key: admission,
                expected,
            },
        ),
        (
            5,
            NativeCommand::AcquireReceipt {
                expected,
                receipt: ReceiptId::from_u128(6),
            },
        ),
        (8, NativeCommand::ReceiveWork { claim, expected }),
        (10, NativeCommand::PostResponse { claim, expected }),
        (11, NativeCommand::ReceiveResponse { claim, expected }),
        (
            12,
            NativeCommand::FailWorkProduction {
                claim,
                slot: 0x01020304,
                diagnostic: ArtifactRef {
                    id: ArtifactId::from_u128(14),
                    hash: ContentHash([42; 32]),
                },
            },
        ),
        (
            14,
            NativeCommand::BeginIncrement {
                claim,
                key: increment,
                expected,
            },
        ),
        (16, NativeCommand::SealIncrementTargets { claim }),
        (17, NativeCommand::EnterWholeWork { claim, expected }),
        (
            18,
            NativeCommand::BeginWork {
                claim,
                key: evaluation(work_target()),
                expected,
            },
        ),
        (
            20,
            NativeCommand::GenerateResultTestament {
                claim,
                id: TestamentId::from_u128(15),
            },
        ),
        (21, NativeCommand::PostResultTestament { expected }),
        (
            22,
            NativeCommand::AdoptReceipt {
                expected,
                previous: receipt(),
                receipt: ReceiptId::from_u128(16),
                holder: ParticipantId::from_u128(17),
            },
        ),
        (23, NativeCommand::ReleaseScope { expected }),
        (
            25,
            NativeCommand::RebindMonitor {
                expected,
                receipt: Some(receipt()),
                id: MonitorId::from_u128(18),
                predecessor: binding(19),
                successor: binding(20),
            },
        ),
        (
            26,
            NativeCommand::CancelMonitor {
                expected,
                receipt: None,
                id: MonitorId::from_u128(18),
            },
        ),
    ]
}

#[test]
fn all_eighteen_fixed_actor_commands_preserve_existing_intents_and_exact_read_budgets() {
    let commands = fixed_commands();
    assert_eq!(commands.len(), 18);
    for (tag, command) in commands {
        let input = request(command);
        let bytes = encode(frame(&input));
        assert_eq!(
            inspect(&bytes).header().kind,
            FrameKind::Request { command: tag }
        );
        roundtrip(frame(&input));
    }
    for command in [
        NativeCommand::RebindMonitor {
            expected: binding(11),
            receipt: None,
            id: MonitorId::from_u128(18),
            predecessor: binding(19),
            successor: binding(20),
        },
        NativeCommand::CancelMonitor {
            expected: binding(11),
            receipt: Some(receipt()),
            id: MonitorId::from_u128(18),
        },
    ] {
        roundtrip(frame(&request(command)));
    }
}

#[test]
fn timer_namespaces_and_all_five_evaluation_targets_preserve_existing_timer_intents() {
    for profile in [
        NativeContentProfile::ProjectionOnly,
        NativeContentProfile::AuthoredV1,
    ] {
        for target in [
            EvaluationTarget::Admission,
            EvaluationTarget::Increment {
                artifact: ArtifactId::from_u128(13),
            },
            work_target(),
            EvaluationTarget::MissingSlot {
                response: TestamentId::from_u128(12),
                slot: 0x01020304,
            },
            EvaluationTarget::Delivery {
                response: TestamentId::from_u128(12),
            },
        ] {
            roundtrip(InputFrame::EvaluationDeadline {
                ledger: LEDGER,
                profile,
                input: NativeDeadlineInput {
                    evaluation: evaluation(target),
                    deadline: deadline(),
                },
            });
        }
        roundtrip(InputFrame::ClaimDeadline {
            ledger: LEDGER,
            profile,
            input: NativeClaimDeadlineInput {
                claim: ClaimId::from_u128(10),
                deadline: deadline(),
            },
        });
        roundtrip(InputFrame::MonitorDeadline {
            ledger: LEDGER,
            profile,
            input: NativeMonitorDeadlineInput {
                claim: ClaimId::from_u128(10),
                monitor: MonitorId::from_u128(18),
                deadline: deadline(),
            },
        });
    }
}

#[test]
fn dynamic_reports_and_monitor_roots_return_none_even_with_zero_decode_allowance() {
    let manifest = [SlotBinding {
        slot: 7,
        artifact: ArtifactRef {
            id: ArtifactId::from_u128(13),
            hash: ContentHash([43; 32]),
        },
    }];
    let report = NativeResponseInput::prepare(
        NativeResponseSpec {
            summary: "Actual reported failure",
            confidence: Confidence::Tentative,
            outcome: OutcomeKind::Failed,
            manifest: &manifest,
            diagnostics: &[],
        },
        NativeLimits::default(),
    )
    .unwrap();
    let charge = report.construction_bytes();
    let report = report.build(charge).unwrap();
    for command in [
        NativeCommand::CloseResponse {
            claim: binding(10),
            response: binding(12),
            report,
        },
        NativeCommand::RegisterMonitor {
            expected: binding(10),
            receipt: Some(receipt()),
            id: MonitorId::from_u128(18),
            roots: vec![WaitPredicate::Satisfied(ClaimId::from_u128(19))],
            deadline: deadline(),
        },
    ] {
        let input = request(command);
        let bytes = encode(frame(&input));
        let inspected = inspect(&bytes);
        assert!(inspected.decode_fixed(0).unwrap().is_none());
        assert!(
            inspected
                .decode_fixed(inspected.quote().visits)
                .unwrap()
                .is_none()
        );
        assert_eq!(inspected.bytes(), bytes);
    }
}

#[test]
fn typed_fixed_decoding_does_not_grant_missing_claim_authority_or_accept_invalid_timers() {
    let input = request(NativeCommand::Post {
        expected: binding(10),
    });
    let bytes = encode(frame(&input));
    let inspected = inspect(&bytes);
    let decoded = inspected
        .decode_fixed(inspected.quote().visits)
        .unwrap()
        .unwrap();
    let FixedFrame::Request {
        ledger,
        profile,
        input,
    } = decoded
    else {
        panic!("actor frame")
    };
    assert_eq!(ledger, LEDGER);
    assert_eq!(profile, NativeContentProfile::ProjectionOnly);
    let key = input.request;
    let core = Core::new_native(
        ledger,
        RangeId(503),
        NativeLimits::default(),
        MemoryBudget::new(64 * 1024 * 1024, 8 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    let mut owner = NativeOwner::new(core).unwrap();
    let budget = owner.budget_stats();
    let range = owner.range_stats();
    assert!(matches!(
        owner.prepare(
            NativeContext {
                principal: Principal::Actor(ACTOR),
                logical_time: 0
            },
            input,
            None
        ),
        Err(NativeOwnerError::Native(NativeError::Contract(
            ContractError::InvalidTarget
        )))
    ));
    assert_eq!(owner.pending_len(), 0);
    assert_eq!(owner.budget_stats(), budget);
    assert_eq!(owner.range_stats(), range);
    assert!(owner.effective().recorded(key).is_none());
    assert!(owner.effective().claim(ClaimId::from_u128(10)).is_none());

    let invalid = NativeDeadlineInput {
        evaluation: EvaluationKey {
            generation: 0,
            ..evaluation(EvaluationTarget::Admission)
        },
        deadline: deadline(),
    };
    let bytes = encode(InputFrame::EvaluationDeadline {
        ledger: LEDGER,
        profile: NativeContentProfile::ProjectionOnly,
        input: invalid,
    });
    let inspected = inspect(&bytes);
    let decoded = inspected
        .decode_fixed(inspected.quote().visits)
        .unwrap()
        .unwrap();
    let FixedFrame::EvaluationDeadline { ledger, input, .. } = decoded else {
        panic!("timer frame")
    };
    assert_eq!(input, invalid);
    assert!(matches!(
        intent::deadline_fingerprint(ledger, input),
        Err(NativeError::Contract(ContractError::InvalidTarget))
    ));
}
