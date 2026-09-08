use super::bytes::{CountingSink, Cursor, SliceSink};
use super::*;
use focal_model::lifecycle::{
    Binding,
    artifact_descriptor::{ArtifactSpec, ContentPointer, Limits, ResultProvenance, WorkProvenance},
    evidence::EvidenceFailure,
    validation::{Attempt, Phase, Target},
};
use focal_model::{
    ArtifactId, ArtifactRef, ClaimId, ContentDomainId, ContentHash, LedgerId, ObjectId, ObjectRef,
    ObjectRevision, ParticipantId, ReceiptFence, ReceiptId, SessionId, TenantId, ValidationId,
    ValidatorId,
};

const LEDGER: LedgerId = LedgerId {
    tenant: TenantId::from_u128(1),
    session: SessionId::from_u128(2),
};
const PRODUCER: ParticipantId = ParticipantId::from_u128(3);
const INPUTS: [ObjectRef; 4] = [
    ObjectRef {
        ledger: LEDGER,
        kind: ObjectKind::Claim,
        id: ObjectId::from_u128(10),
    },
    ObjectRef {
        ledger: LEDGER,
        kind: ObjectKind::Testament,
        id: ObjectId::from_u128(11),
    },
    ObjectRef {
        ledger: LEDGER,
        kind: ObjectKind::Validation,
        id: ObjectId::from_u128(12),
    },
    ObjectRef {
        ledger: LEDGER,
        kind: ObjectKind::Artifact,
        id: ObjectId::from_u128(13),
    },
];

fn spec() -> ArtifactSpec<'static> {
    ArtifactSpec {
        ledger: LEDGER,
        id: ArtifactId::from_u128(4),
        schema: 1,
        kind: "error",
        schema_hash: ContentHash([5; 32]),
        metadata: &[0, 0xff, 7],
        payload: PayloadSpec::Inline(b"actual bytes\0"),
        producer: PRODUCER,
        receipt: Some(ReceiptFence {
            receipt: ReceiptId::from_u128(6),
            epoch: 7,
        }),
        result: None,
        work: None,
        inputs: &INPUTS,
        visibility: &["", "internal", "team/é"],
    }
}

fn build(spec: ArtifactSpec<'_>) -> ArtifactDescriptor {
    ArtifactDescriptor::prepare(
        spec,
        Limits {
            kind_bytes: 32,
            metadata_bytes: 128,
            inline_bytes: 128,
            inputs: 8,
            visibility_labels: 4,
            visibility_label_bytes: 32,
            construction_bytes: 8192,
        },
    )
    .unwrap()
    .build()
    .unwrap()
}

fn encoded(value: &ArtifactDescriptor) -> (Vec<u8>, usize) {
    let mut counter = CountingSink::new(usize::MAX, usize::MAX);
    encode(&mut counter, value).unwrap();
    let mut output = vec![0xcc; counter.len()];
    let mut writer = SliceSink::new(&mut output, counter.visits_used());
    encode(&mut writer, value).unwrap();
    writer.finish().unwrap();
    (output, counter.visits_used())
}

fn binding(id: u128, hash: u8) -> Binding {
    Binding {
        ledger: LEDGER,
        object: ObjectId::from_u128(id),
        content: ContentHash([hash; 32]),
        revision: ObjectRevision(9),
    }
}

fn read_binding(cursor: &mut Cursor<'_>, expected: Binding) -> Result<(), Error> {
    assert_eq!(cursor.fixed::<16>()?, expected.ledger.tenant.0);
    assert_eq!(cursor.fixed::<16>()?, expected.ledger.session.0);
    assert_eq!(cursor.fixed::<16>()?, expected.object.0);
    assert_eq!(cursor.fixed::<32>()?, expected.content.0);
    assert_eq!(cursor.u64()?, expected.revision.0);
    Ok(())
}

fn read_result_target(cursor: &mut Cursor<'_>, expected: Target) -> Result<(), Error> {
    match expected {
        Target::Artifact {
            response,
            slot,
            artifact,
        } => {
            assert_eq!(cursor.u8()?, 0);
            read_binding(cursor, response)?;
            assert_eq!(cursor.u32()?, slot);
            read_binding(cursor, artifact)?;
        }
        Target::Admission { claim } => {
            assert_eq!(cursor.u8()?, 3);
            read_binding(cursor, claim)?;
        }
        Target::Increment { claim, artifact } => {
            assert_eq!(cursor.u8()?, 4);
            read_binding(cursor, claim)?;
            read_binding(cursor, artifact)?;
        }
        Target::MissingSlot { .. } | Target::Delivery { .. } => {
            panic!("authored result target must have a real evaluator")
        }
    }
    Ok(())
}

fn read_body(cursor: &mut Cursor<'_>, expected: ArtifactSpec<'_>) -> Result<(), Error> {
    assert_eq!(cursor.fixed::<16>()?, expected.ledger.tenant.0);
    assert_eq!(cursor.fixed::<16>()?, expected.ledger.session.0);
    assert_eq!(cursor.fixed::<16>()?, expected.id.0);
    assert_eq!(cursor.u16()?, expected.schema);
    assert_eq!(cursor.text(32)?, expected.kind);
    assert_eq!(cursor.fixed::<32>()?, expected.schema_hash.0);
    let metadata = cursor.count(128)?;
    assert_eq!(cursor.take(metadata)?, expected.metadata);
    match expected.payload {
        PayloadSpec::Inline(payload) => {
            assert_eq!(cursor.u8()?, 0);
            let length = cursor.count(128)?;
            assert_eq!(cursor.take(length)?, payload);
        }
        PayloadSpec::Content(pointer) => {
            assert_eq!(cursor.u8()?, 1);
            assert_eq!(cursor.fixed::<16>()?, pointer.domain.0);
            assert_eq!(cursor.fixed::<32>()?, pointer.root.0);
            assert_eq!(cursor.u64()?, pointer.length);
            assert_eq!(
                cursor.u16()?,
                match pointer.class {
                    ContentClass::Document => 1,
                    ContentClass::Evidence => 2,
                    ContentClass::Checkpoint => 3,
                }
            );
        }
    }
    assert_eq!(cursor.fixed::<16>()?, expected.producer.0);
    assert_eq!(cursor.u8()?, u8::from(expected.receipt.is_some()));
    if let Some(receipt) = expected.receipt {
        assert_eq!(cursor.fixed::<16>()?, receipt.receipt.0);
        assert_eq!(cursor.u64()?, receipt.epoch);
    }
    assert_eq!(cursor.u8()?, u8::from(expected.result.is_some()));
    if let Some(result) = expected.result {
        assert_eq!(cursor.fixed::<16>()?, result.claim.0);
        assert_eq!(cursor.fixed::<16>()?, result.validation.0);
        read_result_target(cursor, result.target)?;
        assert_eq!(cursor.u64()?, result.generation);
        assert_eq!(
            cursor.u8()?,
            match result.attempt.phase {
                Phase::Programmatic => 0,
                Phase::Quality => 1,
                Phase::Delivery | Phase::MissingTarget => panic!("authored result phase"),
            }
        );
        assert_eq!(cursor.u32()?, result.attempt.index);
        assert_eq!(cursor.fixed::<16>()?, result.attempt.handler.0);
        assert_eq!(cursor.fixed::<32>()?, result.attempt.version.0);
        assert_eq!(cursor.fixed::<16>()?, result.attempt.evaluator.0);
        assert_eq!(cursor.fixed::<32>()?, result.attempt.definition.0);
        assert_eq!(
            cursor.u8()?,
            match result.value {
                VerdictValue::Pass => 0,
                VerdictValue::Fail => 1,
                VerdictValue::Incomplete => 2,
                VerdictValue::Error => 3,
            }
        );
    }
    assert_eq!(cursor.u8()?, u8::from(expected.work.is_some()));
    if let Some(work) = expected.work {
        assert_eq!(cursor.fixed::<16>()?, work.claim.0);
        assert_eq!(cursor.u32()?, work.cycle);
        let reason = match work.role {
            WorkRole::Output { slot } => {
                assert_eq!(cursor.u8()?, 0);
                assert_eq!(cursor.u32()?, slot);
                None
            }
            WorkRole::Diagnostic { reason } => {
                assert_eq!(cursor.u8()?, 1);
                Some(reason)
            }
            WorkRole::ReceiptRejection { artifact, reason } => {
                assert_eq!(cursor.u8()?, 2);
                assert_eq!(cursor.fixed::<16>()?, artifact.id.0);
                assert_eq!(cursor.fixed::<32>()?, artifact.hash.0);
                Some(reason)
            }
        };
        if let Some(reason) = reason {
            assert_eq!(
                cursor.u8()?,
                match reason {
                    EvidenceFailure::Work => 0,
                    EvidenceFailure::Production => 1,
                    EvidenceFailure::Structure => 2,
                    EvidenceFailure::Metadata => 3,
                }
            );
        }
    }
    assert_eq!(cursor.count(8)?, expected.inputs.len());
    for input in expected.inputs {
        assert_eq!(cursor.fixed::<16>()?, input.ledger.tenant.0);
        assert_eq!(cursor.fixed::<16>()?, input.ledger.session.0);
        assert_eq!(
            cursor.u16()?,
            match input.kind {
                ObjectKind::Claim => 1,
                ObjectKind::Testament => 2,
                ObjectKind::Validation => 3,
                ObjectKind::Artifact => 4,
            }
        );
        assert_eq!(cursor.fixed::<16>()?, input.id.0);
    }
    assert_eq!(cursor.count(4)?, expected.visibility.len());
    for label in expected.visibility {
        assert_eq!(cursor.text(32)?, *label);
    }
    Ok(())
}

#[test]
fn complete_payload_bodies_parse_independently_and_only_external_hashes_are_written() {
    for payload in [
        PayloadSpec::Inline(b"actual bytes\0"),
        PayloadSpec::Inline(&[]),
        PayloadSpec::Content(ContentPointer {
            domain: ContentDomainId::from_u128(30),
            root: ContentHash([31; 32]),
            length: 0x0102030405060708,
            class: ContentClass::Document,
        }),
        PayloadSpec::Content(ContentPointer {
            domain: ContentDomainId::from_u128(30),
            root: ContentHash([31; 32]),
            length: 0,
            class: ContentClass::Evidence,
        }),
        PayloadSpec::Content(ContentPointer {
            domain: ContentDomainId::from_u128(30),
            root: ContentHash([31; 32]),
            length: 8192,
            class: ContentClass::Checkpoint,
        }),
    ] {
        let expected = ArtifactSpec {
            payload,
            receipt: None,
            ..spec()
        };
        let value = build(expected);
        let (output, _) = encoded(&value);
        let mut cursor = Cursor::new(&output, output.len(), usize::MAX).unwrap();
        read_body(&mut cursor, expected).unwrap();
        cursor.finish().unwrap();
    }
    let original = build(spec());
    let changed = build(ArtifactSpec {
        schema_hash: ContentHash([91; 32]),
        ..spec()
    });
    assert_ne!(original.content_hash(), changed.content_hash());
    let (original_bytes, _) = encoded(&original);
    let (changed_bytes, _) = encoded(&changed);
    let schema_start = 16 + 16 + 16 + 2 + 4 + spec().kind.len();
    let schema_end = schema_start + 32;
    assert_eq!(
        &changed_bytes[..schema_start],
        &original_bytes[..schema_start]
    );
    assert_eq!(&changed_bytes[schema_start..schema_end], &[91; 32]);
    assert_eq!(&changed_bytes[schema_end..], &original_bytes[schema_end..]);
}

#[test]
fn result_attempts_and_every_work_role_preserve_exact_authored_provenance() {
    for (target, phase, value) in [
        (
            Target::Artifact {
                response: binding(50, 51),
                slot: 0x01020304,
                artifact: binding(52, 53),
            },
            Phase::Programmatic,
            VerdictValue::Pass,
        ),
        (
            Target::Admission {
                claim: binding(40, 41),
            },
            Phase::Quality,
            VerdictValue::Fail,
        ),
        (
            Target::Increment {
                claim: binding(40, 41),
                artifact: binding(52, 53),
            },
            Phase::Quality,
            VerdictValue::Incomplete,
        ),
        (
            Target::Admission {
                claim: binding(40, 41),
            },
            Phase::Programmatic,
            VerdictValue::Error,
        ),
    ] {
        let result = ResultProvenance {
            claim: ClaimId::from_u128(40),
            validation: ValidationId::from_u128(42),
            target,
            generation: 0x0102030405060708,
            attempt: Attempt {
                phase,
                index: 0x08070605,
                handler: ValidatorId::from_u128(43),
                version: ContentHash([44; 32]),
                evaluator: PRODUCER,
                definition: ContentHash([45; 32]),
            },
            value,
        };
        let expected = ArtifactSpec {
            result: Some(result),
            ..spec()
        };
        let (output, _) = encoded(&build(expected));
        let mut cursor = Cursor::new(&output, output.len(), usize::MAX).unwrap();
        read_body(&mut cursor, expected).unwrap();
        cursor.finish().unwrap();
    }
    for role in [
        WorkRole::Output { slot: 0x01020304 },
        WorkRole::Diagnostic {
            reason: EvidenceFailure::Work,
        },
        WorkRole::Diagnostic {
            reason: EvidenceFailure::Production,
        },
        WorkRole::ReceiptRejection {
            artifact: ArtifactRef {
                id: ArtifactId::from_u128(60),
                hash: ContentHash([61; 32]),
            },
            reason: EvidenceFailure::Structure,
        },
        WorkRole::ReceiptRejection {
            artifact: ArtifactRef {
                id: ArtifactId::from_u128(62),
                hash: ContentHash([63; 32]),
            },
            reason: EvidenceFailure::Metadata,
        },
    ] {
        let expected = ArtifactSpec {
            work: Some(WorkProvenance {
                claim: ClaimId::from_u128(40),
                cycle: 0x01020304,
                role,
            }),
            ..spec()
        };
        let (output, _) = encoded(&build(expected));
        let mut cursor = Cursor::new(&output, output.len(), usize::MAX).unwrap();
        read_body(&mut cursor, expected).unwrap();
        cursor.finish().unwrap();
    }
}

#[test]
fn exact_body_byte_and_visit_allowances_refuse_one_short_without_changing_the_source() {
    let expected = spec();
    let value = build(expected);
    let hash = value.content_hash();
    let pointer = value.metadata().as_ptr();
    let (output, visits) = encoded(&value);
    let mut exact = CountingSink::new(output.len(), visits);
    encode(&mut exact, &value).unwrap();
    for (bytes, visits) in [(output.len() - 1, visits), (output.len(), visits - 1)] {
        let mut counter = CountingSink::new(bytes, visits);
        assert_eq!(encode(&mut counter, &value), Err(Error::Capacity));
        let mut buffer = vec![0xcc; bytes];
        let mut writer = SliceSink::new(&mut buffer, visits);
        assert_eq!(encode(&mut writer, &value), Err(Error::Capacity));
        let written = writer.len();
        assert!(written < output.len());
        assert_eq!(&buffer[..written], &output[..written]);
        assert!(buffer[written..].iter().all(|byte| *byte == 0xcc));
    }
    let mut cursor = Cursor::new(&output, output.len(), usize::MAX).unwrap();
    read_body(&mut cursor, expected).unwrap();
    let read_visits = cursor.visits_used();
    cursor.finish().unwrap();
    let mut exact = Cursor::new(&output, output.len(), read_visits).unwrap();
    read_body(&mut exact, expected).unwrap();
    exact.finish().unwrap();
    let mut short = Cursor::new(&output, output.len(), read_visits - 1).unwrap();
    assert_eq!(read_body(&mut short, expected), Err(Error::Capacity));
    assert!(matches!(
        Cursor::new(&output, output.len() - 1, read_visits),
        Err(Error::Capacity)
    ));
    assert_eq!(value.content_hash(), hash);
    assert_eq!(value.metadata().as_ptr(), pointer);
    assert_eq!(encoded(&value).0, output);
}
