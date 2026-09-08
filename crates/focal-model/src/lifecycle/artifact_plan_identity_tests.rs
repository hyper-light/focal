use super::*;
use crate::{ReceiptId, SessionId, TenantId, ValidatorId};

fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(1),
        session: SessionId::from_u128(2),
    }
}

fn limits() -> Limits {
    Limits {
        kind_bytes: 64,
        metadata_bytes: 1024,
        inline_bytes: 1024,
        inputs: 8,
        visibility_labels: 8,
        visibility_label_bytes: 64,
        construction_bytes: 16 * 1024,
    }
}

fn spec() -> ArtifactSpec<'static> {
    ArtifactSpec {
        ledger: ledger(),
        id: ArtifactId::from_u128(3),
        schema: 1,
        kind: "error",
        schema_hash: ContentHash([4; 32]),
        metadata: b"opaque metadata",
        payload: PayloadSpec::Inline(b"failure description"),
        producer: ParticipantId::from_u128(5),
        receipt: Some(ReceiptFence {
            receipt: ReceiptId::from_u128(6),
            epoch: 1,
        }),
        result: None,
        work: None,
        inputs: &[],
        visibility: &["", "internal", "tenant/one"],
    }
}

fn binding(id: u128) -> Binding {
    Binding {
        ledger: ledger(),
        object: ObjectId::from_u128(id),
        content: ContentHash([7; 32]),
        revision: ObjectRevision(3),
    }
}

fn result() -> ResultProvenance {
    ResultProvenance {
        claim: ClaimId::from_u128(10),
        validation: ValidationId::from_u128(11),
        target: Target::Artifact {
            response: binding(12),
            slot: 2,
            artifact: binding(13),
        },
        generation: 2,
        attempt: Attempt {
            phase: Phase::Programmatic,
            index: 1,
            handler: ValidatorId::from_u128(14),
            version: ContentHash([15; 32]),
            evaluator: spec().producer,
            definition: ContentHash([16; 32]),
        },
        value: VerdictValue::Error,
    }
}

fn pointer() -> ContentPointer {
    ContentPointer {
        domain: ContentDomainId::from_u128(17),
        root: ContentHash([18; 32]),
        length: 8192,
        class: ContentClass::Evidence,
    }
}

/// Check the preallocation identity and an independent owned-field rehash, not
/// only the cached hash that build transfers to its descriptor.
fn identities(spec: ArtifactSpec<'_>) -> (ContentHash, ContentHash) {
    let (plan, content, intent) = bytes::fail_after(0, || {
        let plan = ArtifactDescriptor::prepare(spec, limits()).unwrap();
        let content = plan.content_hash();
        let intent = plan.intent_fingerprint();
        assert_eq!(bytes::remaining_allocations(), Some(0));
        (plan, content, intent)
    });
    let descriptor = plan.build().unwrap();
    assert_eq!(descriptor.content_hash(), content);
    assert_eq!(descriptor.intent_fingerprint(), intent);
    assert_eq!(content_hash(&descriptor), content);
    assert_eq!(descriptor.binding().content, content);
    (content, intent)
}

#[test]
fn checked_borrowed_identity_matches_every_payload_and_work_role() {
    let mut inputs = [
        ObjectKind::Claim,
        ObjectKind::Testament,
        ObjectKind::Validation,
        ObjectKind::Artifact,
    ]
    .map(|kind| ObjectRef {
        ledger: ledger(),
        kind,
        id: ObjectId::from_u128(20),
    });
    inputs.sort();
    let roles = [
        None,
        Some(WorkRole::Output { slot: 3 }),
        Some(WorkRole::Diagnostic {
            reason: EvidenceFailure::Work,
        }),
        Some(WorkRole::Diagnostic {
            reason: EvidenceFailure::Production,
        }),
        Some(WorkRole::Diagnostic {
            reason: EvidenceFailure::Structure,
        }),
        Some(WorkRole::Diagnostic {
            reason: EvidenceFailure::Metadata,
        }),
        Some(WorkRole::ReceiptRejection {
            artifact: ArtifactRef {
                id: ArtifactId::from_u128(21),
                hash: ContentHash([22; 32]),
            },
            reason: EvidenceFailure::Structure,
        }),
        Some(WorkRole::ReceiptRejection {
            artifact: ArtifactRef {
                id: ArtifactId::from_u128(21),
                hash: ContentHash([22; 32]),
            },
            reason: EvidenceFailure::Metadata,
        }),
    ];
    for payload in [
        PayloadSpec::Inline(b""),
        spec().payload,
        PayloadSpec::Content(pointer()),
        PayloadSpec::Content(ContentPointer {
            length: 0,
            class: ContentClass::Document,
            ..pointer()
        }),
        PayloadSpec::Content(ContentPointer {
            class: ContentClass::Checkpoint,
            ..pointer()
        }),
    ] {
        for role in roles {
            identities(ArtifactSpec {
                payload,
                inputs: &inputs,
                work: role.map(|role| WorkProvenance {
                    claim: ClaimId::from_u128(10),
                    cycle: 2,
                    role,
                }),
                ..spec()
            });
        }
    }
    identities(ArtifactSpec {
        receipt: None,
        visibility: &[],
        ..spec()
    });
}

#[test]
fn prepared_result_identity_covers_external_targets_phases_and_verdicts() {
    for target in [
        result().target,
        Target::Admission { claim: binding(10) },
        Target::Increment {
            claim: binding(10),
            artifact: binding(13),
        },
    ] {
        for phase in [Phase::Programmatic, Phase::Quality] {
            for value in [
                VerdictValue::Pass,
                VerdictValue::Fail,
                VerdictValue::Incomplete,
                VerdictValue::Error,
            ] {
                for payload in [spec().payload, PayloadSpec::Content(pointer())] {
                    let provenance = ResultProvenance {
                        target,
                        attempt: Attempt {
                            phase,
                            ..result().attempt
                        },
                        value,
                        ..result()
                    };
                    let checked = ArtifactSpec {
                        result: Some(provenance),
                        payload,
                        ..spec()
                    };
                    let (content, intent) = identities(checked);
                    let attached = ArtifactDescriptor::prepare(
                        ArtifactSpec {
                            result: None,
                            ..checked
                        },
                        limits(),
                    )
                    .unwrap()
                    .build()
                    .unwrap()
                    .with_result_provenance(provenance)
                    .unwrap();
                    assert_eq!(attached.content_hash(), content);
                    assert_eq!(attached.intent_fingerprint(), intent);
                }
            }
        }
    }
}

#[test]
fn changed_borrowed_fields_and_address_have_exact_identity_effects() {
    let base = identities(spec());
    let changed_id = identities(ArtifactSpec {
        id: ArtifactId::from_u128(30),
        ..spec()
    });
    assert_eq!(changed_id.0, base.0);
    assert_ne!(changed_id.1, base.1);
    let input = [ObjectRef {
        ledger: ledger(),
        kind: ObjectKind::Artifact,
        id: ObjectId::from_u128(31),
    }];
    for changed in [
        ArtifactSpec {
            ledger: LedgerId {
                tenant: TenantId::from_u128(30),
                ..ledger()
            },
            ..spec()
        },
        ArtifactSpec {
            kind: "other-error",
            ..spec()
        },
        ArtifactSpec {
            schema_hash: ContentHash([30; 32]),
            ..spec()
        },
        ArtifactSpec {
            metadata: b"changed metadata",
            ..spec()
        },
        ArtifactSpec {
            payload: PayloadSpec::Inline(b"changed payload"),
            ..spec()
        },
        ArtifactSpec {
            producer: ParticipantId::from_u128(30),
            ..spec()
        },
        ArtifactSpec {
            receipt: None,
            ..spec()
        },
        ArtifactSpec {
            inputs: &input,
            ..spec()
        },
        ArtifactSpec {
            visibility: &["internal", "tenant/two"],
            ..spec()
        },
        ArtifactSpec {
            result: Some(result()),
            ..spec()
        },
    ] {
        let changed = identities(changed);
        assert_ne!(changed.0, base.0);
        assert_ne!(changed.1, base.1);
    }
    let result_base = identities(ArtifactSpec {
        result: Some(result()),
        ..spec()
    });
    for provenance in [
        ResultProvenance {
            generation: 3,
            ..result()
        },
        ResultProvenance {
            attempt: Attempt {
                index: 2,
                ..result().attempt
            },
            ..result()
        },
        ResultProvenance {
            target: Target::Artifact {
                response: Binding {
                    revision: ObjectRevision(4),
                    ..binding(12)
                },
                slot: 2,
                artifact: binding(13),
            },
            ..result()
        },
    ] {
        let changed = identities(ArtifactSpec {
            result: Some(provenance),
            ..spec()
        });
        assert_ne!(changed.0, result_base.0);
        assert_ne!(changed.1, result_base.1);
    }
}

#[test]
fn preallocation_identity_survives_build_refusal_and_borrowed_source_release() {
    let (descriptor, content, intent) = {
        let metadata = Vec::from(b"borrowed metadata");
        let payload = Vec::from(b"borrowed payload");
        let checked = ArtifactSpec {
            metadata: &metadata,
            payload: PayloadSpec::Inline(&payload),
            ..spec()
        };
        let plan = bytes::fail_after(0, || {
            ArtifactDescriptor::prepare(checked, limits()).unwrap()
        });
        let content = plan.content_hash();
        let intent = plan.intent_fingerprint();
        assert!(matches!(
            bytes::fail_after(0, || plan.build()),
            Err(ContractError::Capacity)
        ));
        let retry = ArtifactDescriptor::prepare(checked, limits()).unwrap();
        assert_eq!(retry.content_hash(), content);
        assert_eq!(retry.intent_fingerprint(), intent);
        (retry.build().unwrap(), content, intent)
    };
    assert_eq!(content_hash(&descriptor), content);
    assert_eq!(descriptor.intent_fingerprint(), intent);
    assert_eq!(descriptor.metadata(), b"borrowed metadata");
}
