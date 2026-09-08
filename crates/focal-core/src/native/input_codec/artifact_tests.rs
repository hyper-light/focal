//! Typed construction and identity tests; none of these frames grants custody
//! or proves that the surrounding work/report is authorized by an owner.
use super::super::{encode, frame_tests};
use super::*;
use crate::native::intent;
use focal_model::lifecycle::artifact_descriptor::{
    ArtifactDescriptor, ArtifactSpec, ContentPointer, PayloadSpec, ResultProvenance,
    WorkProvenance, WorkRole,
};
use focal_model::{
    ArtifactRef, ContentClass, ContentDomainId, RequestEpoch, RequestId, ValidatorId, VerdictValue,
};

fn input(command: NativeCommand) -> NativeInput {
    NativeInput {
        request: RequestKey {
            principal: ParticipantId::from_u128(13),
            epoch: RequestEpoch(0x0102_0304_0506_0708),
            id: RequestId::from_u128(15),
        },
        command,
    }
}
fn command(tag: u8) -> NativeCommand {
    frame_tests::commands()
        .into_iter()
        .find(|command| encode::tag(command) == tag)
        .unwrap()
}
fn frame(input: &NativeInput) -> InputFrame<'_> {
    frame_tests::frame(input, NativeContentProfile::ProjectionOnly)
}
fn ledger(input: &NativeInput) -> LedgerId {
    let InputFrame::Request { ledger, .. } = frame(input) else {
        panic!("request")
    };
    ledger
}
fn descriptor(input: &NativeInput) -> &ArtifactDescriptor {
    match &input.command {
        NativeCommand::ReportAdmission { artifact, .. }
        | NativeCommand::SubmitWork { artifact, .. }
        | NativeCommand::SubmitDiagnostic { artifact, .. }
        | NativeCommand::RejectWork { artifact, .. }
        | NativeCommand::ReportIncrement { artifact, .. }
        | NativeCommand::ReportWork { artifact, .. } => artifact.get().unwrap(),
        _ => panic!("artifact command"),
    }
}
fn replace(input: &mut NativeInput, descriptor: ArtifactDescriptor) {
    let replacement = NativeArtifactInput::new(descriptor).unwrap();
    match &mut input.command {
        NativeCommand::ReportAdmission { artifact, .. }
        | NativeCommand::SubmitWork { artifact, .. }
        | NativeCommand::SubmitDiagnostic { artifact, .. }
        | NativeCommand::RejectWork { artifact, .. }
        | NativeCommand::ReportIncrement { artifact, .. }
        | NativeCommand::ReportWork { artifact, .. } => *artifact = replacement,
        _ => panic!("artifact command"),
    }
}
fn encoded(input: &NativeInput) -> Vec<u8> {
    let plan = EncodingPlan::prepare(
        frame(input),
        EncodingLimits {
            bytes: 65536,
            visits: 262144,
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
            bytes: 65536,
            visits: 262144,
            items: 1024,
            text_bytes: 4096,
            blob_bytes: 8192,
        },
    )
    .unwrap()
}
fn limits() -> model::Limits {
    model::Limits {
        kind_bytes: 128,
        metadata_bytes: 4096,
        inline_bytes: 4096,
        inputs: 16,
        visibility_labels: 16,
        visibility_label_bytes: 128,
        construction_bytes: 65536,
    }
}
fn capacity<T: std::fmt::Debug>(result: Result<T, DecodeError>) {
    assert!(
        matches!(
            result,
            Err(DecodeError::Codec(CodecError::Capacity))
                | Err(DecodeError::Native(NativeError::Contract(
                    ContractError::Capacity
                )))
                | Err(DecodeError::Native(NativeError::Capacity(_)))
        ),
        "{result:?}"
    );
}
fn quote(bytes: &[u8]) -> ArtifactInputQuote {
    let source = inspected(bytes);
    let mut view = source.artifact_input(usize::MAX).unwrap().unwrap();
    let plan = view
        .prepare(
            NativeLimits::default(),
            limits(),
            usize::MAX,
            usize::MAX,
            usize::MAX,
        )
        .unwrap();
    plan.quote()
}
fn round_trip(input: &NativeInput) {
    let bytes = encoded(input);
    let source = inspected(&bytes);
    let mut view = source.artifact_input(usize::MAX).unwrap().unwrap();
    assert_eq!(view.header(), source.header());
    let plan = view
        .prepare(
            NativeLimits::default(),
            limits(),
            usize::MAX,
            usize::MAX,
            usize::MAX,
        )
        .unwrap();
    let quote = plan.quote();
    assert_eq!(
        plan.intent(),
        intent::fingerprint(ledger(input), input).unwrap()
    );
    assert_eq!(quote.parse_visits, view_parse_visits(&source));
    assert_eq!(quote.source_build_visits, quote.source_inspection_visits);
    let actual = plan
        .build(
            quote.bytes,
            quote.model_build_visits,
            quote.native_build_visits,
        )
        .unwrap();
    assert_eq!(actual.request, input.request);
    assert_eq!(descriptor(&actual), descriptor(input));
    assert_ne!(
        descriptor(&actual).kind().as_ptr(),
        descriptor(input).kind().as_ptr()
    );
    assert_eq!(encoded(&actual), bytes);
    assert_eq!(
        intent::fingerprint(ledger(input), &actual).unwrap(),
        intent::fingerprint(ledger(input), input).unwrap()
    );
    let artifact = match &actual.command {
        NativeCommand::ReportAdmission { artifact, .. }
        | NativeCommand::SubmitWork { artifact, .. }
        | NativeCommand::SubmitDiagnostic { artifact, .. }
        | NativeCommand::RejectWork { artifact, .. }
        | NativeCommand::ReportIncrement { artifact, .. }
        | NativeCommand::ReportWork { artifact, .. } => artifact,
        _ => panic!("artifact command"),
    };
    assert_eq!(artifact.heap_charge().unwrap(), quote.bytes);
    assert_eq!(
        descriptor(&actual).heap_allocations().unwrap() + 1,
        quote.allocations
    );
}
fn view_parse_visits(source: &StructuralInput<'_>) -> usize {
    source
        .artifact_input(usize::MAX)
        .unwrap()
        .unwrap()
        .parse_visits()
}

#[test]
fn all_six_artifact_commands_preserve_complete_fields_native_intents_and_ingress_quotes() {
    for tag in [4, 6, 7, 13, 15, 19] {
        let input = input(command(tag));
        assert_eq!(
            inspected(&encoded(&input)).header().kind,
            FrameKind::Request { command: tag }
        );
        round_trip(&input);
    }
}

#[test]
fn borrowed_construction_preserves_inline_content_and_each_authored_provenance_role() {
    let base = input(command(6));
    let original = descriptor(&base);
    let NativeCommand::SubmitWork { claim, .. } = &base.command else {
        panic!("work")
    };
    let claim = *claim;
    let result = ResultProvenance {
        claim: ClaimId(claim.object.0),
        validation: ValidationId::from_u128(22),
        target: validation::Target::Admission { claim },
        generation: 2,
        attempt: validation::Attempt {
            phase: validation::Phase::Quality,
            index: 3,
            handler: ValidatorId::from_u128(30),
            version: ContentHash([31; 32]),
            evaluator: original.producer(),
            definition: ContentHash([32; 32]),
        },
        value: VerdictValue::Error,
    };
    for payload in [
        PayloadSpec::Inline(b"retained inline bytes"),
        PayloadSpec::Content(ContentPointer {
            domain: ContentDomainId::from_u128(40),
            root: ContentHash([41; 32]),
            length: 12345,
            class: ContentClass::Checkpoint,
        }),
    ] {
        for (tag, result, work) in [
            (6, None, None),
            (4, Some(result), None),
            (6, None, Some(WorkRole::Output { slot: 3 })),
            (
                7,
                None,
                Some(WorkRole::Diagnostic {
                    reason: EvidenceFailure::Production,
                }),
            ),
            (
                13,
                None,
                Some(WorkRole::ReceiptRejection {
                    artifact: ArtifactRef {
                        id: ArtifactId::from_u128(25),
                        hash: ContentHash([42; 32]),
                    },
                    reason: EvidenceFailure::Metadata,
                }),
            ),
        ] {
            let visibility: Vec<_> = original.visibility().collect();
            let built = ArtifactDescriptor::prepare(
                ArtifactSpec {
                    ledger: original.ledger(),
                    id: original.id(),
                    schema: original.schema(),
                    kind: original.kind(),
                    schema_hash: original.schema_hash(),
                    metadata: original.metadata(),
                    payload,
                    producer: original.producer(),
                    receipt: original.receipt(),
                    result,
                    work: work.map(|role| WorkProvenance {
                        claim: ClaimId(claim.object.0),
                        cycle: 2,
                        role,
                    }),
                    inputs: original.inputs(),
                    visibility: &visibility,
                },
                limits(),
            )
            .unwrap()
            .build()
            .unwrap();
            let mut input = input(command(tag));
            replace(&mut input, built);
            round_trip(&input);
        }
    }
}

#[test]
fn each_parse_model_source_native_and_construction_limit_refuses_one_short_and_retries() {
    let input = input(command(6));
    let bytes = encoded(&input);
    let source = inspected(&bytes);
    let q = quote(&bytes);
    assert!(q.source_inspection_visits > 0);
    capacity(source.artifact_input(q.parse_visits - 1));
    assert_eq!(
        source
            .artifact_input(q.parse_visits)
            .unwrap()
            .unwrap()
            .parse_visits(),
        q.parse_visits
    );
    for (model, source_visits, native) in [
        (
            q.model_inspection_visits - 1,
            2 * q.source_inspection_visits,
            q.native_inspection_visits,
        ),
        (
            q.model_inspection_visits,
            q.source_inspection_visits - 1,
            q.native_inspection_visits,
        ),
        (
            q.model_inspection_visits,
            q.source_inspection_visits + q.source_build_visits - 1,
            q.native_inspection_visits,
        ),
        (
            q.model_inspection_visits,
            2 * q.source_inspection_visits,
            q.native_inspection_visits - 1,
        ),
    ] {
        let mut view = source.artifact_input(q.parse_visits).unwrap().unwrap();
        capacity(view.prepare(
            NativeLimits::default(),
            limits(),
            model,
            source_visits,
            native,
        ));
    }
    let mut view = source.artifact_input(q.parse_visits).unwrap().unwrap();
    capacity(view.prepare(
        NativeLimits {
            preparation_bytes: q.bytes - 1,
            ..NativeLimits::default()
        },
        limits(),
        q.model_inspection_visits,
        2 * q.source_inspection_visits,
        q.native_inspection_visits,
    ));
    for (bytes_limit, model_limit, native_limit, source_limit) in [
        (
            q.bytes - 1,
            q.model_build_visits,
            q.native_build_visits,
            2 * q.source_inspection_visits,
        ),
        (
            q.bytes,
            q.model_build_visits - 1,
            q.native_build_visits,
            2 * q.source_inspection_visits,
        ),
        (
            q.bytes,
            q.model_build_visits,
            q.native_build_visits - 1,
            2 * q.source_inspection_visits,
        ),
    ] {
        let mut view = source.artifact_input(q.parse_visits).unwrap().unwrap();
        let plan = view
            .prepare(
                NativeLimits {
                    preparation_bytes: q.bytes,
                    ..NativeLimits::default()
                },
                limits(),
                q.model_inspection_visits,
                source_limit,
                q.native_inspection_visits,
            )
            .unwrap();
        capacity(plan.build(bytes_limit, model_limit, native_limit));
        // A fresh explicit preparation restarts the source allowance after a
        // refused candidate. Build itself must never replenish the shared quota.
        let plan = view
            .prepare(
                NativeLimits {
                    preparation_bytes: q.bytes,
                    ..NativeLimits::default()
                },
                limits(),
                q.model_inspection_visits,
                q.source_inspection_visits + q.source_build_visits,
                q.native_inspection_visits,
            )
            .unwrap();
        assert_eq!(plan.quote(), q);
        let rebuilt = plan
            .build(q.bytes, q.model_build_visits, q.native_build_visits)
            .unwrap();
        assert_eq!(view.source.remaining.get(), 0);
        assert_eq!(encoded(&rebuilt), bytes);
    }
}

fn work_cycle_offset(bytes: &[u8], artifact_start: usize) -> usize {
    let mut cursor = Cursor::new(bytes, bytes.len(), usize::MAX).unwrap();
    cursor.take(artifact_start).unwrap();
    cursor.take(50).unwrap(); // ledger, artifact ID, schema.
    cursor.text(bytes.len()).unwrap();
    cursor.take(32).unwrap(); // external schema pin.
    let metadata = cursor.count(bytes.len()).unwrap();
    cursor.take(metadata).unwrap();
    assert_eq!(cursor.u8().unwrap(), 0); // Inline fixture.
    let payload = cursor.count(bytes.len()).unwrap();
    cursor.take(payload).unwrap();
    cursor.take(16).unwrap(); // producer.
    assert_eq!(cursor.u8().unwrap(), 1);
    cursor.take(24).unwrap(); // receipt + epoch.
    assert_eq!(cursor.u8().unwrap(), 0); // no result provenance.
    assert_eq!(cursor.u8().unwrap(), 1); // work provenance.
    cursor.take(16).unwrap();
    cursor.offset()
}

#[test]
fn structural_bytes_cannot_bypass_artifact_ledger_order_or_provenance_checks() {
    let input = input(command(6));
    let bytes = encoded(&input);
    let artifact_start = 85 + 88 + 4; // fixed actor header, claim binding, slot.
    let source = inspected(&bytes);
    let view = source.artifact_input(usize::MAX).unwrap().unwrap();
    let inputs_offset = view.source.inputs.as_ptr() as usize - bytes.as_ptr() as usize;
    assert!(view.source.input_count >= 2);
    let cycle = work_cycle_offset(&bytes, artifact_start);
    let mut ledger = bytes.clone();
    ledger[artifact_start] ^= 1;
    let mut repeated = bytes.clone();
    let first: [u8; 50] = repeated[inputs_offset..inputs_offset + 50]
        .try_into()
        .unwrap();
    repeated[inputs_offset + 50..inputs_offset + 100].copy_from_slice(&first);
    let mut provenance = bytes.clone();
    provenance[cycle..cycle + 4].copy_from_slice(&0u32.to_le_bytes());
    for (malformed, expected) in [
        (ledger, ContractError::WrongLedger),
        (repeated, ContractError::InvalidManifest),
        (provenance, ContractError::InvalidTarget),
    ] {
        let source = inspected(&malformed);
        let mut view = source.artifact_input(usize::MAX).unwrap().unwrap();
        assert!(
            matches!(view.prepare(NativeLimits::default(), limits(), usize::MAX, usize::MAX, usize::MAX), Err(DecodeError::Native(NativeError::Contract(error))) if error == expected)
        );
    }
    round_trip(&input);
}

fn append_binding(bytes: &mut Vec<u8>, binding: Binding) {
    bytes.extend_from_slice(&binding.ledger.tenant.0);
    bytes.extend_from_slice(&binding.ledger.session.0);
    bytes.extend_from_slice(&binding.object.0);
    bytes.extend_from_slice(&binding.content.0);
    bytes.extend_from_slice(&binding.revision.0.to_le_bytes());
}

/// Independent original request layout; intentionally does not call the shared
/// request_hasher, ArtifactCommand, hash_binding or descriptor-intent helpers.
fn legacy_preimage(input: &NativeInput) -> Vec<u8> {
    let mut bytes = Vec::from(&b"focal/native/request-intent/1"[..]);
    let ledger = ledger(input);
    bytes.extend_from_slice(&ledger.tenant.0);
    bytes.extend_from_slice(&ledger.session.0);
    bytes.extend_from_slice(&input.request.principal.0);
    bytes.extend_from_slice(&input.request.epoch.0.to_le_bytes());
    bytes.extend_from_slice(&input.request.id.0);
    match &input.command {
        NativeCommand::SubmitWork { claim, slot, .. } => {
            bytes.push(6);
            append_binding(&mut bytes, *claim);
            bytes.extend_from_slice(&slot.to_le_bytes());
        }
        NativeCommand::RejectWork {
            claim,
            expected,
            reason,
            ..
        } => {
            bytes.push(13);
            append_binding(&mut bytes, *claim);
            append_binding(&mut bytes, *expected);
            bytes.push(match reason {
                EvidenceFailure::Work => 0,
                EvidenceFailure::Production => 1,
                EvidenceFailure::Structure => 2,
                EvidenceFailure::Metadata => 3,
            });
        }
        NativeCommand::ReportAdmission {
            claim,
            key,
            expected,
            report,
            ..
        } => {
            assert_eq!(key.target, EvaluationTarget::Admission);
            bytes.push(4);
            append_binding(&mut bytes, *claim);
            bytes.extend_from_slice(&key.claim.0);
            bytes.extend_from_slice(&key.validation.0);
            bytes.extend_from_slice(&key.generation.to_le_bytes());
            append_binding(&mut bytes, *expected);
            bytes.extend_from_slice(&report.generation.to_le_bytes());
            bytes.push(match report.attempt.phase {
                validation::Phase::Programmatic => 0,
                validation::Phase::Quality => 1,
                validation::Phase::Delivery => 2,
                validation::Phase::MissingTarget => 3,
            });
            bytes.extend_from_slice(&report.attempt.index.to_le_bytes());
            bytes.extend_from_slice(&report.attempt.handler.0);
            bytes.extend_from_slice(&report.attempt.version.0);
            bytes.extend_from_slice(&report.attempt.evaluator.0);
            bytes.extend_from_slice(&report.attempt.definition.0);
            bytes.push(match report.value {
                VerdictValue::Pass => 0,
                VerdictValue::Fail => 1,
                VerdictValue::Incomplete => 2,
                VerdictValue::Error => 3,
            });
            bytes.extend_from_slice(&report.evidence.id.0);
            bytes.extend_from_slice(&report.evidence.hash.0);
        }
        _ => panic!("independent historical command vector"),
    }
    let descriptor = descriptor(input);
    let mut artifact = blake3::Hasher::new_derive_key("focal/native/artifact-intent/1");
    artifact.update(&descriptor.id().0);
    artifact.update(&descriptor.content_hash().0);
    bytes.extend_from_slice(artifact.finalize().as_bytes());
    bytes
}

#[test]
fn report_submit_and_reject_match_independent_legacy_request_preimages() {
    for tag in [4, 6, 13] {
        let input = input(command(tag));
        let vector = legacy_preimage(&input);
        let expected = ContentHash(*blake3::hash(&vector).as_bytes());
        assert_eq!(
            intent::fingerprint(ledger(&input), &input).unwrap(),
            expected
        );
        let bytes = encoded(&input);
        let source = inspected(&bytes);
        let mut view = source.artifact_input(usize::MAX).unwrap().unwrap();
        let plan = view
            .prepare(
                NativeLimits::default(),
                limits(),
                usize::MAX,
                usize::MAX,
                usize::MAX,
            )
            .unwrap();
        assert_eq!(plan.intent(), expected);
        let q = plan.quote();
        let built = plan
            .build(q.bytes, q.model_build_visits, q.native_build_visits)
            .unwrap();
        assert_eq!(legacy_preimage(&built), vector);
    }
}
