//! Framing and resource tests only. Successful inspection neither admits the
//! authored operations below nor proves their actor, evidence or graph authority.
use super::*;
use focal_model::lifecycle::{
    aggregation, artifact_descriptor, claim::ClaimDefinition, claim_descriptor,
    evidence::SlotBinding, graph, succession, validation_descriptor,
};
use focal_model::{
    ActionType, ArtifactRef, Cause, Confidence, ObjectId, ObjectRef, ObjectRevision, OccurrenceId,
    OutcomeKind, Relation, RelationKind, RelationTarget, RequestEpoch, RequestId, RequirementRef,
    RootCommandId, ScopeKind, SessionId, TenantId, ValidationKind, ValidationMode, ValidationPhase,
    ValidatorId, VerdictValue,
};

const LEDGER: LedgerId = LedgerId {
    tenant: TenantId::from_u128(11),
    session: SessionId::from_u128(12),
};
const ISSUER: ParticipantId = ParticipantId::from_u128(13);
const RESPONDENT: ParticipantId = ParticipantId::from_u128(14);
const SUMMARY: &str = "Failed: é";
const HEADER: usize = 85;
const BINDING: usize = 88;

fn binding(id: u128) -> Binding {
    Binding {
        ledger: LEDGER,
        object: ObjectId::from_u128(id),
        content: ContentHash([0x71; 32]),
        revision: ObjectRevision(1),
    }
}
fn request() -> RequestKey {
    RequestKey {
        principal: ISSUER,
        epoch: RequestEpoch(0x0102_0304_0506_0708),
        id: RequestId::from_u128(15),
    }
}
fn deadline() -> Deadline {
    Deadline {
        timer: TimerId::from_u128(16),
        generation: 0x1122_3344_5566_7788,
        at: 0x8877_6655_4433_2211,
    }
}
fn receipt() -> ReceiptFence {
    ReceiptFence {
        receipt: ReceiptId::from_u128(17),
        epoch: 0x1122_3344,
    }
}
fn evidence(id: u128) -> ArtifactRef {
    ArtifactRef {
        id: ArtifactId::from_u128(id),
        hash: ContentHash([0x91; 32]),
    }
}
fn evaluation(target: EvaluationTarget) -> EvaluationKey {
    EvaluationKey {
        claim: ClaimId::from_u128(21),
        validation: ValidationId::from_u128(22),
        target,
        generation: 23,
    }
}
fn report() -> validation::Report {
    validation::Report {
        generation: 23,
        attempt: validation::Attempt {
            phase: validation::Phase::Programmatic,
            index: 2,
            handler: ValidatorId::from_u128(24),
            version: ContentHash([0xa1; 32]),
            evaluator: ISSUER,
            definition: ContentHash([0xa2; 32]),
        },
        value: VerdictValue::Error,
        evidence: evidence(25),
    }
}
fn artifact() -> NativeArtifactInput {
    let inputs = [
        ObjectRef::claim(LEDGER, ClaimId::from_u128(21)),
        ObjectRef::claim(LEDGER, ClaimId::from_u128(22)),
    ];
    let descriptor = artifact_descriptor::ArtifactDescriptor::prepare(
        artifact_descriptor::ArtifactSpec {
            ledger: LEDGER,
            id: ArtifactId::from_u128(25),
            schema: 1,
            kind: "error",
            schema_hash: ContentHash([0xb1; 32]),
            metadata: &[0, 0xff, 1],
            payload: artifact_descriptor::PayloadSpec::Inline(&[2, 0, 0xff, 3, 4]),
            producer: RESPONDENT,
            receipt: Some(receipt()),
            result: None,
            work: Some(artifact_descriptor::WorkProvenance {
                claim: ClaimId::from_u128(21),
                cycle: 1,
                role: artifact_descriptor::WorkRole::Diagnostic {
                    reason: EvidenceFailure::Work,
                },
            }),
            inputs: &inputs,
            visibility: &["", "a", "é"],
        },
        artifact_descriptor::Limits {
            kind_bytes: 16,
            metadata_bytes: 32,
            inline_bytes: 32,
            inputs: 4,
            visibility_labels: 4,
            visibility_label_bytes: 8,
            construction_bytes: 8192,
        },
    )
    .unwrap()
    .build()
    .unwrap();
    NativeArtifactInput::new(descriptor).unwrap()
}
fn response() -> NativeResponseInput {
    NativeResponseInput {
        summary: SUMMARY.into(),
        confidence: Confidence::Tentative,
        outcome: OutcomeKind::Failed,
        manifest: vec![
            SlotBinding {
                slot: 3,
                artifact: evidence(26),
            },
            SlotBinding {
                slot: 9,
                artifact: evidence(27),
            },
        ],
        diagnostics: vec![evidence(28), evidence(29)],
    }
}
fn delivery(claim: u128, index: u32) -> validation::Declaration {
    validation::Declaration::new(
        Principal::Actor(ISSUER),
        validation::DeclarationSpec {
            binding: binding(100 + claim),
            claim: ClaimId::from_u128(claim),
            issuer: ISSUER,
            declaration_index: index,
            kind: ValidationKind::Receipt,
            phase: ValidationPhase::WholeWork,
            mode: ValidationMode::Required,
            target: validation::TargetDeclaration::Delivery,
            program: validation::Program::Delivery,
            deadline: deadline(),
        },
        validation::Limits {
            handlers: 2,
            attempts: 3,
            slot_bytes: 32,
        },
    )
    .unwrap()
}
fn legacy() -> NativeCommand {
    let declarations = vec![delivery(21, 2)];
    let claim = binding(21);
    let policy = aggregation::AcceptancePolicy::new(
        claim,
        ISSUER,
        &[],
        &declarations,
        aggregation::Limits {
            max_slots: 4,
            max_checks: 4,
            max_results: 8,
            max_updates: 8,
        },
    )
    .unwrap();
    NativeCommand::Create {
        claims: vec![Proposal {
            definition: ClaimDefinition {
                binding: claim,
                issuer: ISSUER,
                subject: RESPONDENT,
                deadline: Some(deadline()),
                max_responses: 3,
                created: SessionSeq(999),
                graph: graph::Declaration::new(
                    &[graph::Obligation {
                        kind: graph::Kind::DependsOn,
                        target: ClaimId::from_u128(30),
                    }],
                    1,
                )
                .unwrap(),
                lineage: succession::Lineage::new(
                    claim,
                    Cause::Root(RootCommandId::from_u128(31)),
                    &[succession::Correction {
                        kind: succession::CorrectionKind::Amends,
                        predecessor: ObjectRef::claim(LEDGER, ClaimId::from_u128(32)),
                    }],
                    1,
                )
                .unwrap(),
                acceptance: policy,
                scope_limits: scope::ScopeLimits {
                    scopes: 2,
                    roots: 3,
                    children: 4,
                },
            },
            owner: None,
        }],
        declarations,
    }
}
fn authored() -> NativeCommand {
    let contributors = [ISSUER];
    let plan = validation_descriptor::ValidationDescriptor::prepare(
        Principal::Actor(ISSUER),
        validation_descriptor::ValidationSpec {
            ledger: LEDGER,
            id: ValidationId::from_u128(121),
            schema: 1,
            claim: ClaimId::from_u128(21),
            issuer: ISSUER,
            declaration_index: 2,
            kind: ValidationKind::Receipt,
            phase: ValidationPhase::WholeWork,
            mode: ValidationMode::Required,
            target: validation::TargetDeclaration::Delivery,
            program: validation::Program::Delivery,
            deadline: deadline(),
            description: "Observe delivery",
            quality_bar: None,
            contributed_by: &contributors,
            policy_revision: 3,
        },
        validation_descriptor::Limits {
            declaration: validation::Limits {
                handlers: 2,
                attempts: 3,
                slot_bytes: 32,
            },
            description_bytes: 64,
            quality_bar_bytes: 64,
            contributors: 4,
            construction_bytes: 8192,
        },
    )
    .unwrap();
    let charge = plan.construction_charge();
    let validation = plan.build(charge).unwrap();
    let mut relations = [
        Relation {
            kind: RelationKind::Issuer,
            target: RelationTarget::Participant(ISSUER),
        },
        Relation {
            kind: RelationKind::Subject,
            target: RelationTarget::Participant(RESPONDENT),
        },
        Relation {
            kind: RelationKind::ClaimAction,
            target: RelationTarget::Action(ActionType::Work),
        },
        Relation {
            kind: RelationKind::CausedBy,
            target: RelationTarget::Root(RootCommandId::from_u128(31)),
        },
    ];
    relations.sort();
    let requirements = [RequirementRef {
        id: ValidationId::from_u128(121),
        specification: validation.specification_hash(),
    }];
    let scopes = [claim_descriptor::ScopeSpec {
        kind: ScopeKind::File,
        key: "src/é",
    }];
    let plan = claim_descriptor::ClaimDescriptor::prepare(
        claim_descriptor::ClaimSpec {
            ledger: LEDGER,
            id: ClaimId::from_u128(21),
            schema: 1,
            occurrence: OccurrenceId::from_u128(33),
            description: "Inspect source",
            relations: &relations,
            scopes: &scopes,
            requirements: &requirements,
            slots: &[],
            deadline: Some(deadline()),
            policy: None,
        },
        claim_descriptor::Limits {
            description_bytes: 64,
            relations: 8,
            scopes: 4,
            scope_key_bytes: 32,
            requirements: 4,
            slots: 4,
            checks: 4,
            construction_bytes: 8192,
        },
    )
    .unwrap();
    let charge = plan.construction_charge();
    let content = plan.build(charge).unwrap();
    NativeCommand::CreateAuthored {
        claims: vec![NativeAuthoredProposal {
            content,
            declarations: vec![validation],
            max_responses: 3,
            scope_limits: scope::ScopeLimits {
                scopes: 2,
                roots: 3,
                children: 4,
            },
            owner: None,
        }],
    }
}

pub(super) fn commands() -> Vec<NativeCommand> {
    let claim = binding(21);
    let expected = binding(22);
    let admission = evaluation(EvaluationTarget::Admission);
    let increment = evaluation(EvaluationTarget::Increment {
        artifact: ArtifactId::from_u128(25),
    });
    let work = evaluation(EvaluationTarget::Work {
        response: TestamentId::from_u128(34),
        slot: 3,
        artifact: ArtifactId::from_u128(25),
    });
    vec![
        legacy(),
        NativeCommand::Cancel { expected },
        NativeCommand::Post { expected },
        NativeCommand::BeginAdmission {
            claim,
            key: admission,
            expected,
        },
        NativeCommand::ReportAdmission {
            claim,
            key: admission,
            expected,
            report: report(),
            artifact: artifact(),
        },
        NativeCommand::AcquireReceipt {
            expected,
            receipt: receipt().receipt,
        },
        NativeCommand::SubmitWork {
            claim,
            slot: 3,
            artifact: artifact(),
        },
        NativeCommand::SubmitDiagnostic {
            claim,
            reason: EvidenceFailure::Work,
            artifact: artifact(),
        },
        NativeCommand::ReceiveWork { claim, expected },
        NativeCommand::CloseResponse {
            claim,
            response: binding(34),
            report: response(),
        },
        NativeCommand::PostResponse { claim, expected },
        NativeCommand::ReceiveResponse { claim, expected },
        NativeCommand::FailWorkProduction {
            claim,
            slot: 3,
            diagnostic: evidence(25),
        },
        NativeCommand::RejectWork {
            claim,
            expected,
            reason: EvidenceFailure::Metadata,
            artifact: artifact(),
        },
        NativeCommand::BeginIncrement {
            claim,
            key: increment,
            expected,
        },
        NativeCommand::ReportIncrement {
            claim,
            key: increment,
            expected,
            report: report(),
            artifact: artifact(),
        },
        NativeCommand::SealIncrementTargets { claim },
        NativeCommand::EnterWholeWork { claim, expected },
        NativeCommand::BeginWork {
            claim,
            key: work,
            expected,
        },
        NativeCommand::ReportWork {
            claim,
            key: work,
            expected,
            report: report(),
            artifact: artifact(),
        },
        NativeCommand::GenerateResultTestament {
            claim,
            id: TestamentId::from_u128(35),
        },
        NativeCommand::PostResultTestament { expected },
        NativeCommand::AdoptReceipt {
            expected,
            previous: receipt(),
            receipt: ReceiptId::from_u128(36),
            holder: RESPONDENT,
        },
        NativeCommand::ReleaseScope { expected },
        NativeCommand::RegisterMonitor {
            expected,
            receipt: Some(receipt()),
            id: MonitorId::from_u128(37),
            roots: vec![
                WaitPredicate::Satisfied(ClaimId::from_u128(38)),
                WaitPredicate::Terminal(ClaimId::from_u128(39)),
                WaitPredicate::Released(ClaimId::from_u128(40)),
            ],
            deadline: deadline(),
        },
        NativeCommand::RebindMonitor {
            expected,
            receipt: Some(receipt()),
            id: MonitorId::from_u128(37),
            predecessor: binding(38),
            successor: binding(39),
        },
        NativeCommand::CancelMonitor {
            expected,
            receipt: None,
            id: MonitorId::from_u128(37),
        },
        authored(),
    ]
}
pub(super) fn frame(input: &NativeInput, profile: NativeContentProfile) -> InputFrame<'_> {
    InputFrame::Request {
        ledger: LEDGER,
        profile,
        input,
    }
}
fn encode(frame: InputFrame<'_>) -> (Vec<u8>, EncodingQuote) {
    let plan = EncodingPlan::prepare(
        frame,
        EncodingLimits {
            bytes: 1 << 20,
            visits: 1 << 20,
        },
    )
    .unwrap();
    let quote = plan.quote();
    let mut bytes = vec![0xcc; quote.bytes];
    plan.write_into(&mut bytes).unwrap();
    (bytes, quote)
}
fn unlimited() -> InspectionLimits {
    InspectionLimits {
        bytes: 1 << 20,
        visits: 1 << 20,
        items: 1 << 16,
        text_bytes: 1 << 16,
        blob_bytes: 1 << 16,
    }
}
fn exact(quote: InspectionQuote) -> InspectionLimits {
    InspectionLimits {
        bytes: quote.bytes,
        visits: quote.visits,
        items: quote.items,
        text_bytes: quote.text_bytes,
        blob_bytes: quote.blob_bytes,
    }
}
fn refusal(bytes: &[u8], limits: InspectionLimits, error: CodecError) {
    assert_eq!(StructuralInput::inspect(bytes, limits).unwrap_err(), error);
}

#[test]
fn all_twenty_eight_actor_frames_preserve_request_headers_and_obey_exact_resource_caps() {
    let values = commands();
    assert_eq!(values.len(), 28);
    for (tag, command) in values.into_iter().enumerate() {
        let profile = if tag == 27 {
            NativeContentProfile::AuthoredV1
        } else {
            NativeContentProfile::ProjectionOnly
        };
        let input = NativeInput {
            request: request(),
            command,
        };
        let source = frame(&input, profile);
        let (bytes, written) = encode(source);
        assert_eq!(bytes[84], u8::try_from(tag).unwrap());
        let inspected = StructuralInput::inspect(&bytes, unlimited()).unwrap();
        assert_eq!(
            inspected.header(),
            InputHeader {
                ledger: LEDGER,
                profile,
                kind: FrameKind::Request {
                    command: u8::try_from(tag).unwrap()
                },
                request: Some(request())
            }
        );
        assert_eq!(inspected.bytes().as_ptr(), bytes.as_ptr());
        assert_eq!(inspected.bytes(), bytes);
        let quote = inspected.quote();
        assert_eq!(quote.bytes, written.bytes);
        if matches!(tag, 0 | 4 | 6 | 7 | 9 | 13 | 15 | 19 | 24 | 27) {
            assert!(inspected.decode_fixed(0).unwrap().is_none());
        } else {
            assert!(inspected.decode_fixed(quote.visits).unwrap().is_some());
        }
        assert_eq!(
            StructuralInput::inspect(&bytes, exact(quote))
                .unwrap()
                .quote(),
            quote
        );
        let mut cap = exact(quote);
        cap.visits -= 1;
        refusal(&bytes, cap, CodecError::Capacity);
        cap = exact(quote);
        cap.bytes -= 1;
        refusal(&bytes, cap, CodecError::Capacity);
        for dimension in 0..3 {
            cap = exact(quote);
            let available = match dimension {
                0 => &mut cap.items,
                1 => &mut cap.text_bytes,
                _ => &mut cap.blob_bytes,
            };
            if *available != 0 {
                *available -= 1;
                refusal(&bytes, cap, CodecError::Capacity);
            }
        }
        assert_eq!(
            EncodingPlan::prepare(
                source,
                EncodingLimits {
                    bytes: written.bytes,
                    visits: written.visits
                }
            )
            .unwrap()
            .quote(),
            written
        );
        assert!(matches!(
            EncodingPlan::prepare(
                source,
                EncodingLimits {
                    bytes: written.bytes,
                    visits: written.visits - 1
                }
            ),
            Err(CodecError::Capacity)
        ));
        assert!(matches!(
            EncodingPlan::prepare(
                source,
                EncodingLimits {
                    bytes: written.bytes - 1,
                    visits: written.visits
                }
            ),
            Err(CodecError::Capacity)
        ));
    }
}

#[test]
fn every_actor_prefix_and_trailing_byte_is_rejected_including_real_creations() {
    for (tag, command) in commands().into_iter().enumerate() {
        let profile = if tag == 27 {
            NativeContentProfile::AuthoredV1
        } else {
            NativeContentProfile::ProjectionOnly
        };
        let input = NativeInput {
            request: request(),
            command,
        };
        let (mut bytes, _) = encode(frame(&input, profile));
        for cut in 0..bytes.len() {
            assert_eq!(
                StructuralInput::inspect(&bytes[..cut], unlimited()).unwrap_err(),
                CodecError::Truncated,
                "command {tag}, prefix {cut}"
            );
        }
        bytes.push(0);
        refusal(&bytes, unlimited(), CodecError::TrailingBytes);
    }
}

/// Test-only independent field oracle. It uses no production codec primitive,
/// and also counts explicit writes so fixed-frame visit quotes are checked.
#[derive(Default)]
struct Vector {
    bytes: Vec<u8>,
    visits: usize,
}
impl Vector {
    fn field(&mut self, bytes: &[u8]) {
        self.visits += 1 + bytes.len();
        self.bytes.extend_from_slice(bytes);
    }
    fn id(&mut self, id: u128) {
        self.field(&id.to_be_bytes());
    }
    fn binding(&mut self, id: u128) {
        self.id(11);
        self.id(12);
        self.id(id);
        self.field(&[0x71; 32]);
        self.field(&1u64.to_le_bytes());
    }
    fn deadline(&mut self) {
        self.id(16);
        self.field(&0x1122_3344_5566_7788u64.to_le_bytes());
        self.field(&0x8877_6655_4433_2211u64.to_le_bytes());
    }
    fn header(namespace: u8, command: Option<u8>) -> Self {
        let mut value = Self::default();
        value.field(b"FCNINPUT");
        value.field(&[1, 0]);
        value.field(&[0]);
        value.field(&[namespace]);
        value.id(11);
        value.id(12);
        if let Some(command) = command {
            value.id(13);
            value.field(&[8, 7, 6, 5, 4, 3, 2, 1]);
            value.id(15);
            value.field(&[command]);
        }
        value
    }
}

#[test]
fn independently_encoded_response_and_monitor_vectors_preserve_all_authored_fields() {
    let input = NativeInput {
        request: request(),
        command: NativeCommand::CloseResponse {
            claim: binding(21),
            response: binding(34),
            report: response(),
        },
    };
    let mut expected = Vector::header(0, Some(9));
    expected.binding(21);
    expected.binding(34);
    expected.field(&10u32.to_le_bytes());
    expected.field(b"Failed: \xc3\xa9");
    expected.field(&[1]);
    expected.field(&[5]);
    expected.field(&2u32.to_le_bytes());
    for (slot, id) in [(3u32, 26u128), (9, 27)] {
        expected.field(&slot.to_le_bytes());
        expected.id(id);
        expected.field(&[0x91; 32]);
    }
    expected.field(&2u32.to_le_bytes());
    for id in [28, 29] {
        expected.id(id);
        expected.field(&[0x91; 32]);
    }
    let (bytes, quote) = encode(frame(&input, NativeContentProfile::ProjectionOnly));
    assert_eq!(bytes, expected.bytes);
    assert_eq!(quote.visits, expected.visits);
    let read = StructuralInput::inspect(&bytes, unlimited())
        .unwrap()
        .quote();
    assert_eq!((read.items, read.text_bytes, read.blob_bytes), (4, 10, 0));
    // Three checked length prefixes, plus a second pass over ten UTF-8 bytes.
    assert_eq!(read.visits, expected.visits + 3 + 11);

    let input = NativeInput {
        request: request(),
        command: NativeCommand::RebindMonitor {
            expected: binding(22),
            receipt: Some(receipt()),
            id: MonitorId::from_u128(37),
            predecessor: binding(38),
            successor: binding(39),
        },
    };
    let mut expected = Vector::header(0, Some(25));
    expected.binding(22);
    expected.field(&[1]);
    expected.id(17);
    expected.field(&0x1122_3344u64.to_le_bytes());
    expected.id(37);
    expected.binding(38);
    expected.binding(39);
    let (bytes, quote) = encode(frame(&input, NativeContentProfile::ProjectionOnly));
    assert_eq!(bytes, expected.bytes);
    assert_eq!(quote.visits, expected.visits);
    assert_eq!(
        StructuralInput::inspect(&bytes, unlimited())
            .unwrap()
            .quote()
            .visits,
        expected.visits
    );
}

#[test]
fn all_timer_namespaces_and_evaluation_targets_have_independent_exact_vectors() {
    let targets = [
        EvaluationTarget::Admission,
        EvaluationTarget::Increment {
            artifact: ArtifactId::from_u128(25),
        },
        EvaluationTarget::Work {
            response: TestamentId::from_u128(34),
            slot: 3,
            artifact: ArtifactId::from_u128(25),
        },
        EvaluationTarget::MissingSlot {
            response: TestamentId::from_u128(34),
            slot: 3,
        },
        EvaluationTarget::Delivery {
            response: TestamentId::from_u128(34),
        },
    ];
    for (tag, target) in targets.into_iter().enumerate() {
        let source = InputFrame::EvaluationDeadline {
            ledger: LEDGER,
            profile: NativeContentProfile::ProjectionOnly,
            input: NativeDeadlineInput {
                evaluation: evaluation(target),
                deadline: deadline(),
            },
        };
        let mut expected = Vector::header(1, None);
        expected.id(21);
        expected.id(22);
        expected.field(&23u64.to_le_bytes());
        expected.field(&[u8::try_from(tag).unwrap()]);
        match tag {
            0 => {}
            1 => expected.id(25),
            2 => {
                expected.id(34);
                expected.field(&3u32.to_le_bytes());
                expected.id(25);
            }
            3 => {
                expected.id(34);
                expected.field(&3u32.to_le_bytes());
            }
            4 => expected.id(34),
            _ => unreachable!(),
        }
        expected.deadline();
        assert_timer(source, expected, FrameKind::EvaluationDeadline);
    }
    let mut expected = Vector::header(2, None);
    expected.id(21);
    expected.deadline();
    assert_timer(
        InputFrame::ClaimDeadline {
            ledger: LEDGER,
            profile: NativeContentProfile::ProjectionOnly,
            input: NativeClaimDeadlineInput {
                claim: ClaimId::from_u128(21),
                deadline: deadline(),
            },
        },
        expected,
        FrameKind::ClaimDeadline,
    );
    let mut expected = Vector::header(3, None);
    expected.id(21);
    expected.id(37);
    expected.deadline();
    assert_timer(
        InputFrame::MonitorDeadline {
            ledger: LEDGER,
            profile: NativeContentProfile::ProjectionOnly,
            input: NativeMonitorDeadlineInput {
                claim: ClaimId::from_u128(21),
                monitor: MonitorId::from_u128(37),
                deadline: deadline(),
            },
        },
        expected,
        FrameKind::MonitorDeadline,
    );
}
fn assert_timer(source: InputFrame<'_>, expected: Vector, kind: FrameKind) {
    let (mut bytes, quote) = encode(source);
    assert_eq!(bytes, expected.bytes);
    assert_eq!(quote.visits, expected.visits);
    let scan = StructuralInput::inspect(&bytes, unlimited()).unwrap();
    assert_eq!(
        scan.header(),
        InputHeader {
            ledger: LEDGER,
            profile: NativeContentProfile::ProjectionOnly,
            kind,
            request: None
        }
    );
    assert_eq!(
        scan.quote(),
        InspectionQuote {
            bytes: bytes.len(),
            visits: expected.visits,
            items: 0,
            text_bytes: 0,
            blob_bytes: 0
        }
    );
    for cut in 0..bytes.len() {
        refusal(&bytes[..cut], unlimited(), CodecError::Truncated);
    }
    bytes.push(0xff);
    refusal(&bytes, unlimited(), CodecError::TrailingBytes);
}

#[test]
fn malformed_headers_closed_tags_options_text_and_lengths_refuse_structurally() {
    let input = NativeInput {
        request: request(),
        command: NativeCommand::CloseResponse {
            claim: binding(21),
            response: binding(34),
            report: response(),
        },
    };
    let (bytes, _) = encode(frame(&input, NativeContentProfile::ProjectionOnly));
    for (offset, value, field) in [
        (0, b'X', "input format"),
        (8, 2, "input format"),
        (10, 2, "content profile"),
        (11, 4, "input namespace"),
        (84, 28, "command"),
        (HEADER + 2 * BINDING + 4 + SUMMARY.len(), 4, "confidence"),
        (
            HEADER + 2 * BINDING + 5 + SUMMARY.len(),
            6,
            "response outcome",
        ),
    ] {
        let mut malformed = bytes.clone();
        malformed[offset] = value;
        refusal(&malformed, unlimited(), CodecError::InvalidTag(field));
    }
    let summary = HEADER + 2 * BINDING;
    let mut malformed = bytes.clone();
    malformed[summary + 4] = 0xff;
    refusal(&malformed, unlimited(), CodecError::InvalidUtf8);
    let mut malformed = bytes.clone();
    malformed[summary..summary + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    refusal(&malformed, unlimited(), CodecError::Capacity);
    let manifest = summary + 4 + SUMMARY.len() + 2;
    let mut malformed = bytes;
    malformed[manifest..manifest + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    refusal(&malformed, unlimited(), CodecError::Capacity);

    let input = NativeInput {
        request: request(),
        command: NativeCommand::CancelMonitor {
            expected: binding(22),
            receipt: None,
            id: MonitorId::from_u128(37),
        },
    };
    let (mut bytes, _) = encode(frame(&input, NativeContentProfile::ProjectionOnly));
    bytes[HEADER + BINDING] = 2;
    refusal(&bytes, unlimited(), CodecError::InvalidTag("option"));

    for (command, profile, wrong) in [
        (legacy(), NativeContentProfile::ProjectionOnly, 1),
        (authored(), NativeContentProfile::AuthoredV1, 0),
    ] {
        let input = NativeInput {
            request: request(),
            command,
        };
        let (mut bytes, _) = encode(frame(&input, profile));
        bytes[10] = wrong;
        refusal(
            &bytes,
            unlimited(),
            CodecError::InvalidTag("creation profile"),
        );
    }
    let input = NativeInput {
        request: request(),
        command: NativeCommand::SubmitDiagnostic {
            claim: binding(21),
            reason: EvidenceFailure::Work,
            artifact: artifact(),
        },
    };
    let (mut bytes, _) = encode(frame(&input, NativeContentProfile::ProjectionOnly));
    let schema = HEADER + BINDING + 1 + 48;
    bytes[schema..schema + 2].copy_from_slice(&2u16.to_le_bytes());
    refusal(
        &bytes,
        unlimited(),
        CodecError::InvalidTag("artifact schema"),
    );
    let input = NativeInput {
        request: request(),
        command: authored(),
    };
    let (mut bytes, _) = encode(frame(&input, NativeContentProfile::AuthoredV1));
    let schema = HEADER + 4 + 48;
    bytes[schema..schema + 2].copy_from_slice(&3u16.to_le_bytes());
    refusal(&bytes, unlimited(), CodecError::InvalidTag("claim schema"));
}

#[test]
fn cumulative_blob_text_and_nested_item_budgets_are_shared_across_fields() {
    let input = NativeInput {
        request: request(),
        command: NativeCommand::SubmitDiagnostic {
            claim: binding(21),
            reason: EvidenceFailure::Work,
            artifact: artifact(),
        },
    };
    let (bytes, _) = encode(frame(&input, NativeContentProfile::ProjectionOnly));
    let quote = StructuralInput::inspect(&bytes, unlimited())
        .unwrap()
        .quote();
    // Two input references plus three labels; kind plus all label bytes;
    // metadata and payload share one byte allowance.
    assert_eq!((quote.items, quote.text_bytes, quote.blob_bytes), (5, 8, 8));
    assert!(StructuralInput::inspect(&bytes, exact(quote)).is_ok());
    for dimension in 0..3 {
        let mut limits = exact(quote);
        match dimension {
            0 => limits.items = 4,
            1 => limits.text_bytes = 7,
            _ => limits.blob_bytes = 7,
        }
        refusal(&bytes, limits, CodecError::Capacity);
    }
    let input = NativeInput {
        request: request(),
        command: authored(),
    };
    let (bytes, _) = encode(frame(&input, NativeContentProfile::AuthoredV1));
    let quote = StructuralInput::inspect(&bytes, unlimited())
        .unwrap()
        .quote();
    // One proposal, four relations, one scope, one pin, one declaration,
    // and one contributor, all drawn from the same item counter.
    assert_eq!(quote.items, 9);
    assert_eq!(
        quote.text_bytes,
        "Inspect source".len() + "src/é".len() + "Observe delivery".len()
    );
    let mut limits = exact(quote);
    limits.items -= 1;
    refusal(&bytes, limits, CodecError::Capacity);
}

#[test]
fn destination_refusal_preserves_sentinels_and_creation_sequence_is_owner_assigned() {
    let mut input = NativeInput {
        request: request(),
        command: legacy(),
    };
    let (bytes, _) = encode(frame(&input, NativeContentProfile::ProjectionOnly));
    let plan = EncodingPlan::prepare(
        frame(&input, NativeContentProfile::ProjectionOnly),
        EncodingLimits {
            bytes: 1 << 20,
            visits: 1 << 20,
        },
    )
    .unwrap();
    for size in [0, bytes.len() - 1, bytes.len() + 1] {
        let mut output = vec![0xcc; size];
        assert_eq!(plan.write_into(&mut output), Err(CodecError::Capacity));
        assert!(output.iter().all(|byte| *byte == 0xcc));
    }
    let mut output = vec![0xcc; bytes.len() + 2];
    plan.write_into(&mut output[1..1 + bytes.len()]).unwrap();
    assert_eq!(&output[1..1 + bytes.len()], &bytes);
    assert_eq!((output[0], output[output.len() - 1]), (0xcc, 0xcc));
    let NativeCommand::Create { claims, .. } = &mut input.command else {
        panic!("legacy fixture");
    };
    claims[0].definition.created = SessionSeq(123_456);
    assert_eq!(
        encode(frame(&input, NativeContentProfile::ProjectionOnly)).0,
        bytes
    );
    let NativeCommand::Create { declarations, .. } = &mut input.command else {
        panic!("legacy fixture");
    };
    declarations[0] = delivery(21, 3);
    assert!(matches!(
        EncodingPlan::prepare(
            frame(&input, NativeContentProfile::ProjectionOnly),
            EncodingLimits {
                bytes: 1 << 20,
                visits: 1 << 20
            }
        ),
        Err(CodecError::InvalidTag("acceptance correspondence"))
    ));
}

#[test]
fn inspection_does_not_mistake_structural_framing_for_semantic_admission() {
    let input = NativeInput {
        request: request(),
        command: NativeCommand::Cancel {
            expected: binding(22),
        },
    };
    let (mut bytes, _) = encode(frame(&input, NativeContentProfile::ProjectionOnly));
    // Actor and target validity belong to checked owner admission. Even these
    // deliberately unusable identities are preserved by the structural seam.
    bytes[44..60].fill(0);
    bytes[HEADER..].fill(0);
    let inspected = StructuralInput::inspect(&bytes, unlimited()).unwrap();
    assert_eq!(
        inspected.header().request.unwrap().principal,
        ParticipantId::from_u128(0)
    );
    assert_eq!(inspected.bytes(), bytes);
}
