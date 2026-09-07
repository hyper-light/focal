use super::*;
use crate::{ReceiptId, SessionId, TenantId};

fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(1),
        session: SessionId::from_u128(2),
    }
}

fn limits() -> Limits {
    Limits {
        kind_bytes: 128,
        metadata_bytes: 1024,
        inline_bytes: 1024,
        inputs: 16,
        visibility_labels: 16,
        visibility_label_bytes: 128,
        construction_bytes: 16 * 1024,
    }
}

fn spec() -> ArtifactSpec<'static> {
    ArtifactSpec {
        ledger: ledger(),
        id: ArtifactId::from_u128(3),
        schema: 1,
        kind: "validation/error-report",
        schema_hash: ContentHash([4; 32]),
        metadata: br#"{"format":"diagnostic"}"#,
        payload: PayloadSpec::Inline(b"the requested dependency was unavailable"),
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

fn inputs() -> Vec<ObjectRef> {
    let mut inputs: Vec<_> = [
        ObjectKind::Claim,
        ObjectKind::Testament,
        ObjectKind::Validation,
        ObjectKind::Artifact,
    ]
    .into_iter()
    .map(|kind| ObjectRef {
        ledger: ledger(),
        kind,
        id: ObjectId::from_u128(10),
    })
    .collect();
    inputs.sort();
    inputs
}

fn build(spec: ArtifactSpec<'_>) -> ArtifactDescriptor {
    ArtifactDescriptor::prepare(spec, limits())
        .unwrap()
        .build()
        .unwrap()
}

fn pointer() -> ContentPointer {
    ContentPointer {
        domain: ContentDomainId::from_u128(30),
        root: ContentHash([31; 32]),
        length: 8192,
        class: ContentClass::Evidence,
    }
}

#[test]
fn descriptor_owns_all_borrowed_data_and_copy_outlives_original() {
    let original = {
        let kind = String::from("validation/error-report");
        let metadata = Vec::from(br#"{"format":"diagnostic"}"#);
        let payload = Vec::from(b"the requested dependency was unavailable");
        let inputs = inputs();
        let labels = [
            String::new(),
            String::from("internal"),
            String::from("tenant/one"),
        ];
        let labels: Vec<_> = labels.iter().map(String::as_str).collect();
        let original = build(ArtifactSpec {
            kind: &kind,
            metadata: &metadata,
            payload: PayloadSpec::Inline(&payload),
            inputs: &inputs,
            visibility: &labels,
            ..spec()
        });
        assert_ne!(original.kind().as_ptr(), kind.as_ptr());
        assert_ne!(original.metadata().as_ptr(), metadata.as_ptr());
        assert_ne!(original.inputs().as_ptr(), inputs.as_ptr());
        original
    };
    assert_eq!(original.id(), spec().id);
    assert_eq!(original.ledger(), ledger());
    assert_eq!(original.schema(), 1);
    assert_eq!(original.kind(), spec().kind);
    assert_eq!(original.schema_hash(), spec().schema_hash);
    assert_eq!(original.metadata(), spec().metadata);
    assert_eq!(original.payload(), spec().payload);
    assert_eq!(original.producer(), spec().producer);
    assert_eq!(original.receipt(), spec().receipt);
    assert_eq!(original.inputs(), inputs());
    assert_eq!(original.visibility().collect::<Vec<_>>(), spec().visibility);
    let hash = original.content_hash();
    let intent = original.intent_fingerprint();
    let binding = original.binding();
    let copied = original.try_copy(original.copy_charge().unwrap()).unwrap();
    assert_eq!(copied, original);
    assert_ne!(copied.kind().as_ptr(), original.kind().as_ptr());
    assert_ne!(copied.metadata().as_ptr(), original.metadata().as_ptr());
    assert_ne!(copied.inputs().as_ptr(), original.inputs().as_ptr());
    drop(original);
    assert_eq!(copied.content_hash(), hash);
    assert_eq!(copied.intent_fingerprint(), intent);
    assert_eq!(copied.binding(), binding);
    assert_eq!(binding.revision, ObjectRevision(1));
    assert_eq!(copied.payload(), spec().payload);
    assert_eq!(copied.inputs(), inputs());
}

#[test]
fn content_identity_excludes_own_id_but_retry_identity_includes_it() {
    let first = build(spec());
    let repeated = build(spec());
    let other_id = build(ArtifactSpec {
        id: ArtifactId::from_u128(4),
        ..spec()
    });
    assert_eq!(first.content_hash(), repeated.content_hash());
    assert_eq!(first.intent_fingerprint(), repeated.intent_fingerprint());
    assert_eq!(first.content_hash(), other_id.content_hash());
    assert_ne!(first.binding().object, other_id.binding().object);
    assert_ne!(first.intent_fingerprint(), other_id.intent_fingerprint());
    let split_a = build(ArtifactSpec {
        metadata: b"ab",
        payload: PayloadSpec::Inline(b"c"),
        ..spec()
    });
    let split_b = build(ArtifactSpec {
        metadata: b"a",
        payload: PayloadSpec::Inline(b"bc"),
        ..spec()
    });
    assert_ne!(split_a.content_hash(), split_b.content_hash());
}

#[test]
fn every_immutable_content_field_changes_the_native_identity() {
    let first = build(spec());
    let reference = [ObjectRef {
        ledger: ledger(),
        kind: ObjectKind::Claim,
        id: ObjectId::from_u128(10),
    }];
    let changed = [
        ArtifactSpec {
            ledger: LedgerId {
                tenant: TenantId::from_u128(99),
                ..ledger()
            },
            ..spec()
        },
        ArtifactSpec {
            ledger: LedgerId {
                session: SessionId::from_u128(99),
                ..ledger()
            },
            ..spec()
        },
        ArtifactSpec {
            kind: "validation/error-report-v2",
            ..spec()
        },
        ArtifactSpec {
            schema_hash: ContentHash([99; 32]),
            ..spec()
        },
        ArtifactSpec {
            metadata: b"different metadata",
            ..spec()
        },
        ArtifactSpec {
            payload: PayloadSpec::Inline(b"different failure"),
            ..spec()
        },
        ArtifactSpec {
            payload: PayloadSpec::Content(pointer()),
            ..spec()
        },
        ArtifactSpec {
            producer: ParticipantId::from_u128(99),
            ..spec()
        },
        ArtifactSpec {
            receipt: None,
            ..spec()
        },
        ArtifactSpec {
            receipt: Some(ReceiptFence {
                receipt: ReceiptId::from_u128(99),
                epoch: 1,
            }),
            ..spec()
        },
        ArtifactSpec {
            receipt: Some(ReceiptFence {
                receipt: ReceiptId::from_u128(6),
                epoch: 99,
            }),
            ..spec()
        },
        ArtifactSpec {
            inputs: &reference,
            ..spec()
        },
        ArtifactSpec {
            visibility: &["", "internal", "tenant/two"],
            ..spec()
        },
        ArtifactSpec {
            visibility: &["internal", "tenant/one"],
            ..spec()
        },
    ];
    for (index, changed) in changed.into_iter().enumerate() {
        let changed = build(changed);
        assert_ne!(
            first.content_hash(),
            changed.content_hash(),
            "field {index}"
        );
        assert_ne!(
            first.intent_fingerprint(),
            changed.intent_fingerprint(),
            "field {index}"
        );
    }
    let claim = build(ArtifactSpec {
        inputs: &reference,
        ..spec()
    });
    for kind in [
        ObjectKind::Testament,
        ObjectKind::Validation,
        ObjectKind::Artifact,
    ] {
        let changed = [ObjectRef {
            kind,
            ..reference[0]
        }];
        assert_ne!(
            claim.content_hash(),
            build(ArtifactSpec {
                inputs: &changed,
                ..spec()
            })
            .content_hash()
        );
    }
    let changed = [ObjectRef {
        id: ObjectId::from_u128(11),
        ..reference[0]
    }];
    assert_ne!(
        claim.content_hash(),
        build(ArtifactSpec {
            inputs: &changed,
            ..spec()
        })
        .content_hash()
    );
}

#[test]
fn content_pointers_are_retained_by_value_and_hash_every_store_coordinate() {
    let base = build(ArtifactSpec {
        payload: PayloadSpec::Content(pointer()),
        ..spec()
    });
    for changed in [
        ContentPointer {
            domain: ContentDomainId::from_u128(99),
            ..pointer()
        },
        ContentPointer {
            root: ContentHash([99; 32]),
            ..pointer()
        },
        ContentPointer {
            length: 0,
            ..pointer()
        },
        ContentPointer {
            length: u64::MAX,
            ..pointer()
        },
        ContentPointer {
            class: ContentClass::Document,
            ..pointer()
        },
        ContentPointer {
            class: ContentClass::Checkpoint,
            ..pointer()
        },
    ] {
        let original = build(ArtifactSpec {
            payload: PayloadSpec::Content(changed),
            ..spec()
        });
        assert_ne!(base.content_hash(), original.content_hash());
        let copied = original.try_copy(original.copy_charge().unwrap()).unwrap();
        assert_eq!(copied, original);
        drop(original);
        assert_eq!(copied.payload(), PayloadSpec::Content(changed));
    }
    let inline_empty = build(ArtifactSpec {
        payload: PayloadSpec::Inline(&[]),
        ..spec()
    });
    let content_empty = build(ArtifactSpec {
        payload: PayloadSpec::Content(ContentPointer {
            length: 0,
            ..pointer()
        }),
        ..spec()
    });
    assert_ne!(inline_empty.content_hash(), content_empty.content_hash());
}

#[test]
fn canonical_input_ir_rejects_duplicates_unsorted_and_foreign_references_before_allocation() {
    let sorted = inputs();
    let mut unsorted = inputs();
    unsorted.reverse();
    let duplicate = [sorted[0], sorted[0]];
    let foreign = [ObjectRef {
        ledger: LedgerId {
            session: SessionId::from_u128(99),
            ..ledger()
        },
        ..sorted[0]
    }];
    let zero = [ObjectRef {
        id: ObjectId::from_u128(0),
        ..sorted[0]
    }];
    let cases = [
        (
            ArtifactSpec {
                inputs: &unsorted,
                ..spec()
            },
            ContractError::InvalidManifest,
        ),
        (
            ArtifactSpec {
                inputs: &duplicate,
                ..spec()
            },
            ContractError::InvalidManifest,
        ),
        (
            ArtifactSpec {
                inputs: &foreign,
                ..spec()
            },
            ContractError::WrongLedger,
        ),
        (
            ArtifactSpec {
                inputs: &zero,
                ..spec()
            },
            ContractError::InvalidTarget,
        ),
        (
            ArtifactSpec {
                visibility: &["z", "a"],
                ..spec()
            },
            ContractError::InvalidManifest,
        ),
        (
            ArtifactSpec {
                visibility: &["a", "a"],
                ..spec()
            },
            ContractError::InvalidManifest,
        ),
    ];
    for (spec, expected) in cases {
        bytes::fail_after(9, || {
            assert_eq!(
                ArtifactDescriptor::prepare(spec, limits()).unwrap_err(),
                expected
            );
            assert_eq!(bytes::remaining_allocations(), Some(9));
        });
    }
    assert_eq!(
        build(ArtifactSpec {
            inputs: &sorted,
            ..spec()
        })
        .inputs(),
        sorted
    );
}

#[test]
fn invalid_descriptor_coordinates_cannot_receive_a_computed_binding() {
    let cases = [
        (
            ArtifactSpec {
                ledger: LedgerId {
                    tenant: TenantId::from_u128(0),
                    ..ledger()
                },
                ..spec()
            },
            ContractError::WrongLedger,
        ),
        (
            ArtifactSpec {
                ledger: LedgerId {
                    session: SessionId::from_u128(0),
                    ..ledger()
                },
                ..spec()
            },
            ContractError::WrongLedger,
        ),
        (
            ArtifactSpec {
                id: ArtifactId::from_u128(0),
                ..spec()
            },
            ContractError::InvalidTarget,
        ),
        (
            ArtifactSpec {
                producer: ParticipantId::from_u128(0),
                ..spec()
            },
            ContractError::InvalidTarget,
        ),
        (
            ArtifactSpec {
                schema: 0,
                ..spec()
            },
            ContractError::InvalidPolicy,
        ),
        (
            ArtifactSpec {
                schema: 2,
                ..spec()
            },
            ContractError::InvalidPolicy,
        ),
        (
            ArtifactSpec { kind: "", ..spec() },
            ContractError::InvalidPolicy,
        ),
        (
            ArtifactSpec {
                kind: "error Report",
                ..spec()
            },
            ContractError::InvalidPolicy,
        ),
        (
            ArtifactSpec {
                schema_hash: ContentHash([0; 32]),
                ..spec()
            },
            ContractError::InvalidPolicy,
        ),
        (
            ArtifactSpec {
                receipt: Some(ReceiptFence {
                    receipt: ReceiptId::from_u128(0),
                    epoch: 1,
                }),
                ..spec()
            },
            ContractError::StaleReceipt,
        ),
        (
            ArtifactSpec {
                receipt: Some(ReceiptFence {
                    receipt: ReceiptId::from_u128(6),
                    epoch: 0,
                }),
                ..spec()
            },
            ContractError::StaleReceipt,
        ),
        (
            ArtifactSpec {
                payload: PayloadSpec::Content(ContentPointer {
                    domain: ContentDomainId::from_u128(0),
                    ..pointer()
                }),
                ..spec()
            },
            ContractError::InvalidTarget,
        ),
        (
            ArtifactSpec {
                payload: PayloadSpec::Content(ContentPointer {
                    root: ContentHash([0; 32]),
                    ..pointer()
                }),
                ..spec()
            },
            ContractError::InvalidTarget,
        ),
    ];
    for (spec, expected) in cases {
        assert_eq!(
            ArtifactDescriptor::prepare(spec, limits()).unwrap_err(),
            expected
        );
    }
}

#[test]
fn all_resource_bounds_are_checked_without_allocating_owned_buffers() {
    let inputs = inputs();
    let spec = ArtifactSpec {
        inputs: &inputs,
        ..spec()
    };
    let plan = ArtifactDescriptor::prepare(spec, limits()).unwrap();
    let exact = plan.construction_charge();
    for limits in [
        Limits {
            kind_bytes: spec.kind.len() - 1,
            ..limits()
        },
        Limits {
            metadata_bytes: spec.metadata.len() - 1,
            ..limits()
        },
        Limits {
            inline_bytes: 1,
            ..limits()
        },
        Limits {
            inputs: 3,
            ..limits()
        },
        Limits {
            visibility_labels: 2,
            ..limits()
        },
        Limits {
            visibility_label_bytes: 1,
            ..limits()
        },
        Limits {
            construction_bytes: exact - 1,
            ..limits()
        },
    ] {
        bytes::fail_after(9, || {
            assert!(matches!(
                ArtifactDescriptor::prepare(spec, limits),
                Err(ContractError::Capacity)
            ));
            assert_eq!(bytes::remaining_allocations(), Some(9));
        });
    }
    bytes::fail_after(9, || {
        let exact_plan = ArtifactDescriptor::prepare(
            spec,
            Limits {
                construction_bytes: exact,
                ..limits()
            },
        )
        .unwrap();
        assert_eq!(bytes::remaining_allocations(), Some(9));
        assert_eq!(exact_plan.construction_heap_allocations(), 7);
        assert_eq!(
            exact_plan.construction_heap_bytes() + size_of::<ArtifactDescriptor>(),
            exact
        );
    });
    let built = plan.build().unwrap();
    assert_eq!(built.retained_bytes().unwrap(), exact);
    assert_eq!(built.heap_allocations().unwrap(), 7);
}

#[test]
fn every_partial_build_and_copy_failure_leaves_retry_and_original_intact() {
    let inputs = inputs();
    let spec = ArtifactSpec {
        inputs: &inputs,
        ..spec()
    };
    let original = build(spec);
    let hash = original.content_hash();
    let intent = original.intent_fingerprint();
    let charge = original.copy_charge().unwrap();
    let count = original.copy_heap_allocations().unwrap();
    assert_eq!(count, 7);
    bytes::fail_after(9, || {
        assert!(matches!(
            original.try_copy(charge - 1),
            Err(ContractError::Capacity)
        ));
        assert_eq!(bytes::remaining_allocations(), Some(9));
    });
    for after in 0..count {
        let plan = ArtifactDescriptor::prepare(spec, limits()).unwrap();
        assert!(
            matches!(
                bytes::fail_after(after, || plan.build()),
                Err(ContractError::Capacity)
            ),
            "build allocation {after}"
        );
        assert!(
            matches!(
                bytes::fail_after(after, || original.try_copy(charge)),
                Err(ContractError::Capacity)
            ),
            "copy allocation {after}"
        );
        assert_eq!(original.content_hash(), hash);
        assert_eq!(original.intent_fingerprint(), intent);
        assert_eq!(original.payload(), spec.payload);
        let retry = build(spec);
        assert_eq!(retry, original);
        let copied = original.try_copy(charge).unwrap();
        assert_eq!(copied, original);
    }
}

#[test]
fn copy_compacts_all_spare_capacities_and_preserves_computed_identity() {
    let inputs = inputs();
    let mut original = build(ArtifactSpec {
        inputs: &inputs,
        ..spec()
    });
    original.kind.reserve_exact(80);
    original.metadata.reserve_exact(80);
    if let Payload::Inline(payload) = &mut original.payload {
        payload.reserve_exact(80);
    }
    original.inputs.reserve_exact(8);
    original.visibility.reserve_exact(8);
    for label in &mut original.visibility {
        label.reserve_exact(80);
    }
    assert_eq!(original.heap_allocations().unwrap(), 8);
    assert_eq!(original.copy_heap_allocations().unwrap(), 7);
    assert!(original.retained_heap_bytes().unwrap() > original.copy_heap_bytes().unwrap());
    let copied = original.try_copy(original.copy_charge().unwrap()).unwrap();
    assert_eq!(copied, original);
    assert_eq!(
        copied.retained_heap_bytes().unwrap(),
        copied.copy_heap_bytes().unwrap()
    );
    assert_eq!(copied.heap_allocations().unwrap(), 7);
    assert_eq!(copied.intent_fingerprint(), original.intent_fingerprint());
}

fn target_binding(id: u128) -> Binding {
    Binding {
        ledger: ledger(),
        object: ObjectId::from_u128(id),
        content: ContentHash([17; 32]),
        revision: ObjectRevision(1),
    }
}

fn result() -> ResultProvenance {
    ResultProvenance {
        claim: ClaimId::from_u128(10),
        validation: ValidationId::from_u128(20),
        target: Target::Artifact {
            response: target_binding(30),
            slot: 0,
            artifact: target_binding(40),
        },
        generation: 1,
        attempt: Attempt {
            phase: Phase::Programmatic,
            index: 0,
            handler: crate::ValidatorId::from_u128(50),
            version: ContentHash([51; 32]),
            evaluator: spec().producer,
            definition: ContentHash([52; 32]),
        },
        value: VerdictValue::Error,
    }
}

#[test]
fn result_provenance_distinguishes_retries_without_copying_owned_buffers() {
    let inputs = inputs();
    let plain = build(ArtifactSpec {
        inputs: &inputs,
        ..spec()
    });
    let old_hash = plain.content_hash();
    let old_intent = plain.intent_fingerprint();
    let kind = plain.kind().as_ptr();
    let metadata = plain.metadata().as_ptr();
    let references = plain.inputs().as_ptr();
    let heap = plain.retained_heap_bytes().unwrap();
    let allocations = plain.heap_allocations().unwrap();
    let first = bytes::fail_after(0, || plain.with_result_provenance(result())).unwrap();
    assert_ne!(first.content_hash(), old_hash);
    assert_ne!(first.intent_fingerprint(), old_intent);
    assert_eq!(first.result_provenance(), Some(result()));
    assert_eq!(first.kind().as_ptr(), kind);
    assert_eq!(first.metadata().as_ptr(), metadata);
    assert_eq!(first.inputs().as_ptr(), references);
    assert_eq!(first.retained_heap_bytes().unwrap(), heap);
    assert_eq!(first.heap_allocations().unwrap(), allocations);
    let direct = build(ArtifactSpec {
        inputs: &inputs,
        result: Some(result()),
        ..spec()
    });
    assert_eq!(direct, first);
    let again = bytes::fail_after(0, || first.with_result_provenance(result())).unwrap();
    assert_eq!(again, direct);
    assert_eq!(again.kind().as_ptr(), kind);
    let retry = ResultProvenance {
        attempt: Attempt {
            index: 1,
            ..result().attempt
        },
        ..result()
    };
    let next = build(ArtifactSpec {
        inputs: &inputs,
        result: Some(retry),
        ..spec()
    });
    assert_eq!(again.payload(), next.payload());
    assert_ne!(again.content_hash(), next.content_hash());
    let copy = again.try_copy(again.copy_charge().unwrap()).unwrap();
    assert_eq!(copy, again);
    assert!(matches!(
        again.with_result_provenance(retry),
        Err(ContractError::ContentConflict)
    ));
    assert_eq!(copy.result_provenance(), Some(result()));
    assert_eq!(copy, direct);
}

#[test]
fn result_identity_covers_each_attempt_field_verdict_and_exact_target_coordinate() {
    let baseline = build(ArtifactSpec {
        result: Some(result()),
        ..spec()
    });
    let mut variants = vec![
        ResultProvenance {
            claim: ClaimId::from_u128(11),
            ..result()
        },
        ResultProvenance {
            validation: ValidationId::from_u128(21),
            ..result()
        },
        ResultProvenance {
            generation: 2,
            ..result()
        },
        ResultProvenance {
            attempt: Attempt {
                phase: Phase::Quality,
                ..result().attempt
            },
            ..result()
        },
        ResultProvenance {
            attempt: Attempt {
                index: 1,
                ..result().attempt
            },
            ..result()
        },
        ResultProvenance {
            attempt: Attempt {
                handler: crate::ValidatorId::from_u128(51),
                ..result().attempt
            },
            ..result()
        },
        ResultProvenance {
            attempt: Attempt {
                version: ContentHash([53; 32]),
                ..result().attempt
            },
            ..result()
        },
        ResultProvenance {
            attempt: Attempt {
                definition: ContentHash([53; 32]),
                ..result().attempt
            },
            ..result()
        },
        ResultProvenance {
            value: VerdictValue::Pass,
            ..result()
        },
        ResultProvenance {
            value: VerdictValue::Fail,
            ..result()
        },
        ResultProvenance {
            value: VerdictValue::Incomplete,
            ..result()
        },
    ];
    let response = target_binding(30);
    let artifact = target_binding(40);
    for changed in [
        Target::Artifact {
            response: target_binding(31),
            slot: 0,
            artifact,
        },
        Target::Artifact {
            response: Binding {
                content: ContentHash([18; 32]),
                ..response
            },
            slot: 0,
            artifact,
        },
        Target::Artifact {
            response: Binding {
                revision: ObjectRevision(2),
                ..response
            },
            slot: 0,
            artifact,
        },
        Target::Artifact {
            response,
            slot: 1,
            artifact,
        },
        Target::Artifact {
            response,
            slot: 0,
            artifact: target_binding(41),
        },
        Target::Artifact {
            response,
            slot: 0,
            artifact: Binding {
                content: ContentHash([18; 32]),
                ..artifact
            },
        },
        Target::Artifact {
            response,
            slot: 0,
            artifact: Binding {
                revision: ObjectRevision(2),
                ..artifact
            },
        },
        Target::Admission {
            claim: target_binding(10),
        },
        Target::Increment {
            claim: target_binding(10),
            artifact,
        },
    ] {
        variants.push(ResultProvenance {
            target: changed,
            ..result()
        });
    }
    for (index, result) in variants.into_iter().enumerate() {
        let changed = build(ArtifactSpec {
            result: Some(result),
            ..spec()
        });
        assert_ne!(
            changed.content_hash(),
            baseline.content_hash(),
            "result field {index}"
        );
        assert_ne!(
            changed.intent_fingerprint(),
            baseline.intent_fingerprint(),
            "result field {index}"
        );
    }
    // Automatic target outcomes have no external result artifacts. Their hash
    // branches remain exhaustive even though the constructor rejects the role.
    for target in [
        Target::MissingSlot { response, slot: 0 },
        Target::Delivery { response },
    ] {
        let mut hash_probe = baseline.try_copy(baseline.copy_charge().unwrap()).unwrap();
        hash_probe.result.as_mut().unwrap().target = target;
        assert_ne!(content_hash(&hash_probe), baseline.content_hash());
    }
    // Evaluator and producer must agree at admission. Probe the private hash
    // directly to ensure both coordinates are encoded, rather than accidentally
    // relying only on the producer field to distinguish a result's evaluator.
    let mut hash_probe = baseline.try_copy(baseline.copy_charge().unwrap()).unwrap();
    hash_probe.result.as_mut().unwrap().attempt.evaluator = ParticipantId::from_u128(99);
    assert_ne!(content_hash(&hash_probe), baseline.content_hash());
}

#[test]
fn result_provenance_rejects_forged_coordinates_before_allocation_or_role_binding() {
    let response = target_binding(30);
    let artifact = target_binding(40);
    let foreign = Binding {
        ledger: LedgerId {
            session: SessionId::from_u128(99),
            ..ledger()
        },
        ..artifact
    };
    let mut cases = vec![
        (
            ResultProvenance {
                claim: ClaimId::from_u128(0),
                ..result()
            },
            ContractError::InvalidTarget,
        ),
        (
            ResultProvenance {
                validation: ValidationId::from_u128(0),
                ..result()
            },
            ContractError::InvalidTarget,
        ),
        (
            ResultProvenance {
                generation: 0,
                ..result()
            },
            ContractError::StaleEvaluation,
        ),
        (
            ResultProvenance {
                attempt: Attempt {
                    evaluator: ParticipantId::from_u128(99),
                    ..result().attempt
                },
                ..result()
            },
            ContractError::WrongActor,
        ),
        (
            ResultProvenance {
                attempt: Attempt {
                    handler: crate::ValidatorId::from_u128(0),
                    ..result().attempt
                },
                ..result()
            },
            ContractError::InvalidPolicy,
        ),
        (
            ResultProvenance {
                attempt: Attempt {
                    version: ContentHash([0; 32]),
                    ..result().attempt
                },
                ..result()
            },
            ContractError::InvalidPolicy,
        ),
        (
            ResultProvenance {
                attempt: Attempt {
                    definition: ContentHash([0; 32]),
                    ..result().attempt
                },
                ..result()
            },
            ContractError::InvalidPolicy,
        ),
        (
            ResultProvenance {
                attempt: Attempt {
                    phase: Phase::Delivery,
                    ..result().attempt
                },
                ..result()
            },
            ContractError::InvalidPolicy,
        ),
        (
            ResultProvenance {
                attempt: Attempt {
                    phase: Phase::MissingTarget,
                    ..result().attempt
                },
                ..result()
            },
            ContractError::InvalidPolicy,
        ),
    ];
    for (target, error) in [
        (
            Target::MissingSlot { response, slot: 0 },
            ContractError::InvalidTarget,
        ),
        (Target::Delivery { response }, ContractError::InvalidTarget),
        (
            Target::Artifact {
                response: foreign,
                slot: 0,
                artifact,
            },
            ContractError::WrongLedger,
        ),
        (
            Target::Artifact {
                response,
                slot: 0,
                artifact: foreign,
            },
            ContractError::WrongLedger,
        ),
        (
            Target::MissingSlot {
                response: foreign,
                slot: 0,
            },
            ContractError::WrongLedger,
        ),
        (
            Target::Delivery { response: foreign },
            ContractError::WrongLedger,
        ),
        (
            Target::Admission { claim: foreign },
            ContractError::WrongLedger,
        ),
        (
            Target::Increment {
                claim: target_binding(10),
                artifact: foreign,
            },
            ContractError::WrongLedger,
        ),
        (
            Target::Admission {
                claim: target_binding(11),
            },
            ContractError::InvalidTarget,
        ),
        (
            Target::Increment {
                claim: target_binding(11),
                artifact,
            },
            ContractError::InvalidTarget,
        ),
        (
            Target::Artifact {
                response,
                slot: 0,
                artifact: Binding {
                    object: ObjectId::from_u128(0),
                    ..artifact
                },
            },
            ContractError::InvalidTarget,
        ),
        (
            Target::Artifact {
                response,
                slot: 0,
                artifact: Binding {
                    content: ContentHash([0; 32]),
                    ..artifact
                },
            },
            ContractError::InvalidTarget,
        ),
        (
            Target::Artifact {
                response,
                slot: 0,
                artifact: Binding {
                    revision: ObjectRevision(0),
                    ..artifact
                },
            },
            ContractError::InvalidTarget,
        ),
    ] {
        cases.push((ResultProvenance { target, ..result() }, error));
    }
    for (result, expected) in cases {
        let plain = build(spec());
        bytes::fail_after(9, || {
            assert_eq!(
                ArtifactDescriptor::prepare(
                    ArtifactSpec {
                        result: Some(result),
                        ..spec()
                    },
                    limits()
                )
                .unwrap_err(),
                expected
            );
            assert_eq!(bytes::remaining_allocations(), Some(9));
            assert_eq!(plain.with_result_provenance(result).unwrap_err(), expected);
            assert_eq!(bytes::remaining_allocations(), Some(9));
        });
    }
}

fn work_provenance() -> WorkProvenance {
    WorkProvenance {
        claim: ClaimId::from_u128(700),
        cycle: 1,
        role: WorkRole::Output { slot: 0 },
    }
}

#[test]
fn work_identity_distinguishes_claim_cycle_slot_and_diagnostic_reason_without_metadata_changes() {
    let base = work_provenance();
    let original = build(ArtifactSpec {
        kind: "error",
        work: Some(base),
        ..spec()
    });
    let mut hashes = std::collections::BTreeSet::new();
    hashes.insert(original.content_hash());
    for provenance in [
        WorkProvenance {
            claim: ClaimId::from_u128(701),
            ..base
        },
        WorkProvenance { cycle: 2, ..base },
        WorkProvenance {
            role: WorkRole::Output { slot: 1 },
            ..base
        },
        WorkProvenance {
            role: WorkRole::Diagnostic {
                reason: EvidenceFailure::Work,
            },
            ..base
        },
        WorkProvenance {
            role: WorkRole::Diagnostic {
                reason: EvidenceFailure::Production,
            },
            ..base
        },
        WorkProvenance {
            role: WorkRole::Diagnostic {
                reason: EvidenceFailure::Structure,
            },
            ..base
        },
        WorkProvenance {
            role: WorkRole::Diagnostic {
                reason: EvidenceFailure::Metadata,
            },
            ..base
        },
    ] {
        let changed = build(ArtifactSpec {
            kind: "error",
            work: Some(provenance),
            ..spec()
        });
        assert_eq!(changed.payload(), original.payload());
        assert_eq!(changed.metadata(), original.metadata());
        assert!(hashes.insert(changed.content_hash()));
        assert_ne!(changed.intent_fingerprint(), original.intent_fingerprint());
    }
    let new_id = build(ArtifactSpec {
        id: ArtifactId::from_u128(999),
        kind: "error",
        work: Some(base),
        ..spec()
    });
    assert_eq!(new_id.content_hash(), original.content_hash());
    assert_ne!(new_id.intent_fingerprint(), original.intent_fingerprint());
}

#[test]
fn work_role_binding_moves_buffers_and_copy_preserves_exact_provenance() {
    let plain = build(spec());
    let kind = plain.kind().as_ptr();
    let metadata = plain.metadata().as_ptr();
    let old_hash = plain.content_hash();
    let first = bytes::fail_after(0, || plain.with_work_provenance(work_provenance())).unwrap();
    assert_eq!(first.kind().as_ptr(), kind);
    assert_eq!(first.metadata().as_ptr(), metadata);
    assert_ne!(first.content_hash(), old_hash);
    let direct = build(ArtifactSpec {
        work: Some(work_provenance()),
        ..spec()
    });
    assert_eq!(first, direct);
    let again = bytes::fail_after(0, || first.with_work_provenance(work_provenance())).unwrap();
    assert_eq!(again, direct);
    assert_eq!(again.kind().as_ptr(), kind);
    let copy = again.try_copy(again.copy_charge().unwrap()).unwrap();
    assert_eq!(copy.work_provenance(), Some(work_provenance()));
    assert_eq!(copy, direct);
    assert_eq!(
        again
            .with_work_provenance(WorkProvenance {
                cycle: 2,
                ..work_provenance()
            })
            .unwrap_err(),
        ContractError::ContentConflict
    );
    assert_eq!(
        copy.with_result_provenance(result()).unwrap_err(),
        ContractError::InvalidPolicy
    );
    let result_bound = build(ArtifactSpec {
        result: Some(result()),
        ..spec()
    });
    assert_eq!(
        result_bound
            .with_work_provenance(work_provenance())
            .unwrap_err(),
        ContractError::InvalidPolicy
    );
}

#[test]
fn invalid_work_roles_refuse_before_allocating_or_changing_any_identity() {
    let base = work_provenance();
    for (invalid, expected) in [
        (
            ArtifactSpec {
                work: Some(WorkProvenance {
                    claim: ClaimId::from_u128(0),
                    ..base
                }),
                ..spec()
            },
            ContractError::InvalidTarget,
        ),
        (
            ArtifactSpec {
                work: Some(WorkProvenance { cycle: 0, ..base }),
                ..spec()
            },
            ContractError::InvalidTarget,
        ),
        (
            ArtifactSpec {
                work: Some(WorkProvenance {
                    cycle: u32::MAX,
                    ..base
                }),
                ..spec()
            },
            ContractError::InvalidTarget,
        ),
        (
            ArtifactSpec {
                receipt: None,
                work: Some(base),
                ..spec()
            },
            ContractError::StaleReceipt,
        ),
        (
            ArtifactSpec {
                result: Some(result()),
                work: Some(base),
                ..spec()
            },
            ContractError::InvalidPolicy,
        ),
        (
            ArtifactSpec {
                work: Some(WorkProvenance {
                    role: WorkRole::Diagnostic {
                        reason: EvidenceFailure::Work,
                    },
                    ..base
                }),
                ..spec()
            },
            ContractError::MissingEvidence,
        ),
    ] {
        bytes::fail_after(0, || {
            assert_eq!(
                ArtifactDescriptor::prepare(invalid, limits()).unwrap_err(),
                expected
            );
            assert_eq!(bytes::remaining_allocations(), Some(0));
        });
    }
}

#[test]
fn claimant_rejection_identity_names_exact_work_and_cannot_be_a_respondent_diagnostic() {
    let target = ArtifactRef {
        id: ArtifactId::from_u128(800),
        hash: ContentHash([81; 32]),
    };
    let provenance = WorkProvenance {
        role: WorkRole::ReceiptRejection {
            artifact: target,
            reason: EvidenceFailure::Structure,
        },
        ..work_provenance()
    };
    let original = build(ArtifactSpec {
        kind: "error",
        work: Some(provenance),
        ..spec()
    });
    let mut hashes = std::collections::BTreeSet::new();
    hashes.insert(original.content_hash());
    for role in [
        WorkRole::ReceiptRejection {
            artifact: ArtifactRef {
                id: ArtifactId::from_u128(801),
                ..target
            },
            reason: EvidenceFailure::Structure,
        },
        WorkRole::ReceiptRejection {
            artifact: ArtifactRef {
                hash: ContentHash([82; 32]),
                ..target
            },
            reason: EvidenceFailure::Structure,
        },
        WorkRole::ReceiptRejection {
            artifact: target,
            reason: EvidenceFailure::Metadata,
        },
        WorkRole::Diagnostic {
            reason: EvidenceFailure::Structure,
        },
    ] {
        let changed = build(ArtifactSpec {
            kind: "error",
            work: Some(WorkProvenance { role, ..provenance }),
            ..spec()
        });
        assert_eq!(changed.metadata(), original.metadata());
        assert_eq!(changed.payload(), original.payload());
        assert!(hashes.insert(changed.content_hash()));
    }
    let plain = build(ArtifactSpec {
        kind: "error",
        ..spec()
    });
    let metadata = plain.metadata().as_ptr();
    let attached = bytes::fail_after(0, || plain.with_work_provenance(provenance)).unwrap();
    assert_eq!(attached.metadata().as_ptr(), metadata);
    assert_eq!(attached, original);
    assert_eq!(
        attached.try_copy(attached.copy_charge().unwrap()).unwrap(),
        original
    );
}

#[test]
fn malformed_claimant_rejection_roles_refuse_before_allocation() {
    let target = ArtifactRef {
        id: ArtifactId::from_u128(800),
        hash: ContentHash([81; 32]),
    };
    for (artifact, reason, expected) in [
        (target, EvidenceFailure::Work, ContractError::InvalidPolicy),
        (
            target,
            EvidenceFailure::Production,
            ContractError::InvalidPolicy,
        ),
        (
            ArtifactRef {
                id: ArtifactId::from_u128(0),
                ..target
            },
            EvidenceFailure::Structure,
            ContractError::InvalidTarget,
        ),
        (
            ArtifactRef {
                hash: ContentHash([0; 32]),
                ..target
            },
            EvidenceFailure::Metadata,
            ContractError::InvalidTarget,
        ),
    ] {
        let work = WorkProvenance {
            role: WorkRole::ReceiptRejection { artifact, reason },
            ..work_provenance()
        };
        bytes::fail_after(0, || {
            assert_eq!(
                ArtifactDescriptor::prepare(
                    ArtifactSpec {
                        kind: "error",
                        work: Some(work),
                        ..spec()
                    },
                    limits()
                )
                .unwrap_err(),
                expected
            );
            assert_eq!(bytes::remaining_allocations(), Some(0));
        });
    }
    assert_eq!(
        ArtifactDescriptor::prepare(
            ArtifactSpec {
                work: Some(WorkProvenance {
                    role: WorkRole::ReceiptRejection {
                        artifact: target,
                        reason: EvidenceFailure::Structure
                    },
                    ..work_provenance()
                }),
                ..spec()
            },
            limits()
        )
        .unwrap_err(),
        ContractError::MissingEvidence
    );
}
