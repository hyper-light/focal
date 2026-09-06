use super::*;
use focal_core::Core;

fn id(value: u128) -> String {
    format!("{value:032x}")
}
fn context() -> BuildContext {
    BuildContext {
        ledger: LedgerId {
            tenant: TenantId::from_u128(1),
            session: SessionId::from_u128(2),
        },
        actor: ParticipantId::from_u128(3),
        root: RootCommandId::from_u128(4),
        policy_revision: 1,
    }
}
fn ids() -> impl IdGenerator {
    let mut next = 100_u128;
    move || {
        next = next.checked_add(1).ok_or(InputError::Identity)?;
        Ok(next.to_be_bytes())
    }
}
fn receipt_validation() -> ValidationDocument {
    ValidationDocument {
        id: Some(id(11)),
        kind: "receipt".into(),
        phase: "whole_work".into(),
        mode: "required".into(),
        description: "Receipt of the testament".into(),
        evaluator: id(5),
        quality_bar: None,
        handlers: vec![],
        evidence_schemas: vec![],
        contributed_by: vec![id(3)],
        policy_revision: Some(1),
    }
}
fn claim() -> ClaimDocument {
    ClaimDocument {
        id: Some(id(10)),
        occurrence: Some(id(12)),
        description: "Check the report".into(),
        target: id(6),
        action: "work".into(),
        scopes: vec![ScopeDocument {
            kind: "file".into(),
            key: "report.json".into(),
        }],
        relations: vec![],
        deadline: None,
        validations: vec![receipt_validation()],
    }
}
fn envelope(context: &BuildContext, command: Command, request: u128) -> AuthenticatedInput {
    AuthenticatedInput {
        ledger: context.ledger,
        principal: context.actor,
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(request),
        expected_revision: None,
        authority: AuthorityContext {
            runtime: matches!(command, Command::NegotiateEpoch { .. }),
            cause: Cause::Root(context.root),
            policy_revision: context.policy_revision,
            logical_time: 100,
            evidence: vec![],
        },
        command,
    }
}
fn apply(core: &mut Core, input: AuthenticatedInput) -> focal_core::ApplyResult {
    let prepared = core.prepare(&input).unwrap();
    core.apply_serial(SessionSeq(core.sequence().0 + 1), prepared)
        .unwrap()
}

#[test]
fn flags_json_yaml_expand_to_identical_admissible_claim() {
    let flags = claim();
    let json = serde_json::to_vec(&flags).unwrap();
    let yaml = format!(
        "id: '{}'\noccurrence: '{}'\ndescription: Check the report\ntarget: '{}'\nscopes:\n  - kind: file\n    key: report.json\nvalidations:\n  - id: '{}'\n    kind: receipt\n    phase: whole_work\n    mode: required\n    description: Receipt of the testament\n    evaluator: '{}'\n    contributed_by: ['{}']\n    policy_revision: 1\n",
        id(10),
        id(12),
        id(6),
        id(11),
        id(5),
        id(3)
    );
    let from_json: ClaimDocument = parse_document(&json, InputFormat::Json).unwrap();
    let from_yaml: ClaimDocument = parse_document(yaml.as_bytes(), InputFormat::Yaml).unwrap();
    let context = context();
    let commands =
        [flags, from_json, from_yaml].map(|doc| doc.build(&context, &mut ids()).unwrap());
    assert_eq!(commands[0], commands[1]);
    assert_eq!(commands[1], commands[2]);
    assert_eq!(
        postcard::to_allocvec(&commands[0]).unwrap(),
        postcard::to_allocvec(&commands[2]).unwrap()
    );
    let Command::GenerateClaim { claim } = &commands[0] else {
        panic!("claim")
    };
    assert_eq!(claim.content.issuer(), Some(context.actor));
    assert_eq!(claim.content.cause(), Some(Cause::Root(context.root)));
    assert_eq!(
        claim.content.requirements[0].specification,
        claim.validations[0].content.specification_hash().unwrap()
    );
    let mut core = Core::new(context.ledger, Limits::default());
    apply(
        &mut core,
        envelope(
            &context,
            Command::NegotiateEpoch {
                epoch: RequestEpoch(1),
            },
            1,
        ),
    );
    let result = apply(&mut core, envelope(&context, commands[0].clone(), 2));
    assert_eq!(result.receipt.sequence, SessionSeq(2));
    assert_eq!(core.snapshot().claims.len(), 1);
}

#[test]
fn parsers_reject_duplicates_authority_aliases_depth_and_trailing_input() {
    let encoded = serde_json::to_string(&claim()).unwrap();
    for field in [
        "runtime",
        "issuer",
        "cause",
        "status",
        "logical_time",
        "durable",
    ] {
        let bad = format!("{{\"{field}\":true,{}", encoded.strip_prefix('{').unwrap());
        assert!(
            parse_document::<ClaimDocument>(bad.as_bytes(), InputFormat::Json).is_err(),
            "{field}"
        );
    }
    for bad in [
        b"{\"x\":1,\"x\":2}".as_slice(),
        br#"{"x": {"a":1,"\u0061":2}}"#,
        br#"{} {}"#,
    ] {
        assert!(parse_document::<serde_json::Value>(bad, InputFormat::Json).is_err());
    }
    for bad in [
        "x: 1\nx: 2\n",
        "x: &a [1]\ny: *a\n",
        "---\nx: 1\n---\nx: 2\n",
        "x: !malicious foo\n",
        "x: {<<: {z: 1}}\n",
    ] {
        assert!(
            parse_document::<serde_json::Value>(bad.as_bytes(), InputFormat::Yaml).is_err(),
            "{bad}"
        );
    }
    let deep = format!("{}0{}", "[".repeat(64), "]".repeat(64));
    assert!(parse_document::<serde_json::Value>(deep.as_bytes(), InputFormat::Json).is_err());
    let wide = format!("[{}]", vec!["0"; 9000].join(","));
    assert!(parse_document::<serde_json::Value>(wide.as_bytes(), InputFormat::Json).is_err());
    assert!(matches!(
        parse_document::<ClaimDocument>(&vec![b' '; MAX_INPUT_BYTES + 1], InputFormat::Json),
        Err(InputError::Capacity)
    ));
    assert!(parse_document::<ClaimDocument>(b"{}", InputFormat::Json).is_err());
    assert!(parse_document::<TestamentDocument>(b"{}", InputFormat::Yaml).is_err());
}

#[test]
fn ids_defaults_generator_failures_and_mandatory_validation_are_explicit() {
    assert_eq!(
        parse_id(&id(255).to_uppercase()).unwrap(),
        255_u128.to_be_bytes()
    );
    for value in [id(0), "xyz".into(), format!(" {}", id(1)), "g".repeat(32)] {
        assert!(parse_id(&value).is_err());
    }
    assert!(parse_hash(&"0".repeat(64)).is_err());
    let mut doc = claim();
    doc.id = None;
    doc.occurrence = None;
    doc.validations[0].id = None;
    let command = doc.clone().build(&context(), &mut ids()).unwrap();
    let Command::GenerateClaim { claim } = command else {
        panic!("claim")
    };
    assert_eq!(claim.id, ClaimId::from_u128(101));
    assert_eq!(claim.content.occurrence, OccurrenceId::from_u128(102));
    assert_eq!(claim.validations[0].id, ValidationId::from_u128(103));
    assert_eq!(
        doc.clone()
            .build(&context(), &mut || Err(InputError::Identity)),
        Err(InputError::Identity)
    );
    assert_eq!(
        doc.build(&context(), &mut || Ok([0; 16])),
        Err(InputError::Identity)
    );
    let mut doc = super::tests::claim();
    doc.validations.clear();
    assert!(doc.build(&context(), &mut ids()).is_err());
    let mut doc = super::tests::claim();
    doc.validations[0].id = doc.id.clone();
    assert!(doc.build(&context(), &mut ids()).is_err());
}

#[test]
fn receipt_contract_rejects_authored_quality_handlers_and_evidence() {
    assert!(claim().build(&context(), &mut ids()).is_ok());
    for field in 0..3 {
        let mut doc = claim();
        let receipt = &mut doc.validations[0];
        match field {
            0 => receipt.quality_bar = Some("Prove correctness".into()),
            1 => receipt.handlers.push(HandlerDocument {
                id: id(16),
                version: "ab".repeat(32),
                agentic: false,
            }),
            _ => receipt.evidence_schemas.push("ab".repeat(32)),
        }
        assert!(matches!(
            doc.build(&context(), &mut ids()),
            Err(InputError::Invalid(message)) if message.contains("delivery only")
        ));
    }
}

#[test]
fn validator_contract_never_invents_handlers_or_changes_pinned_policy() {
    let mut doc = claim();
    let mut test = receipt_validation();
    test.id = Some(id(15));
    test.kind = "test".into();
    doc.validations.push(test.clone());
    assert!(doc.clone().build(&context(), &mut ids()).is_err());
    test.handlers = vec![HandlerDocument {
        id: id(16),
        version: "ab".repeat(32),
        agentic: false,
    }];
    test.quality_bar = Some("Readable output".into());
    doc.validations[1] = test.clone();
    assert!(doc.clone().build(&context(), &mut ids()).is_err());
    test.handlers.push(HandlerDocument {
        id: id(17),
        version: "cd".repeat(32),
        agentic: true,
    });
    doc.validations[1] = test;
    assert!(doc.clone().build(&context(), &mut ids()).is_ok());
    doc.validations[1].policy_revision = Some(2);
    assert!(doc.build(&context(), &mut ids()).is_err());
}

#[test]
fn artifact_and_testament_documents_pin_payload_producer_and_manifest() {
    let context = context();
    let artifact = ArtifactDocument {
        claim: id(10),
        receipt: ReceiptDocument {
            id: id(20),
            epoch: 1,
        },
        evidence_set: id(21),
        id: Some(id(22)),
        kind: "test-report".into(),
        schema_hash: "ab".repeat(32),
        metadata: vec![],
        payload: PayloadDocument::Text {
            text: "{\"passed\":1}".into(),
        },
        inputs: vec![ObjectReferenceDocument {
            kind: "claim".into(),
            id: id(10),
        }],
        visibility: vec![],
    };
    let json = serde_json::to_vec(&artifact).unwrap();
    let decoded: ArtifactDocument = parse_document(&json, InputFormat::Json).unwrap();
    let command = decoded.build(&context, &mut ids()).unwrap();
    let Command::AttachArtifact { artifact, .. } = command else {
        panic!("artifact")
    };
    assert_eq!(artifact.content.producer, context.actor);
    assert_eq!(
        artifact.content.receipt,
        Some(ReceiptFence {
            receipt: ReceiptId::from_u128(20),
            epoch: 1
        })
    );
    let hash = artifact.content.content_hash().unwrap();
    let close = TestamentDocument {
        id: Some(id(23)),
        claim: id(10),
        receipt: ReceiptDocument {
            id: id(20),
            epoch: 1,
        },
        evidence_set: id(21),
        manifest: vec![ArtifactReferenceDocument {
            id: id(22),
            hash: hash.to_string(),
        }],
        summary: "Evidence attached".into(),
        confidence: "committed".into(),
        outcome: "complete".into(),
    };
    let json = serde_json::to_vec(&close).unwrap();
    let decoded: TestamentDocument = parse_document(&json, InputFormat::Json).unwrap();
    assert_eq!(
        close.clone().build(&context, &mut ids()).unwrap(),
        decoded.build(&context, &mut ids()).unwrap()
    );
    let mut duplicate = close;
    duplicate.manifest.push(duplicate.manifest[0].clone());
    assert!(duplicate.build(&context, &mut ids()).is_err());
}

#[test]
fn testament_builder_is_admitted_by_core_without_granting_artifact_custody() {
    let issuer = context();
    let worker = BuildContext {
        actor: ParticipantId::from_u128(6),
        ..issuer
    };
    let mut core = Core::new(issuer.ledger, Limits::default());
    apply(
        &mut core,
        envelope(
            &issuer,
            Command::NegotiateEpoch {
                epoch: RequestEpoch(1),
            },
            1,
        ),
    );
    apply(
        &mut core,
        envelope(
            &worker,
            Command::NegotiateEpoch {
                epoch: RequestEpoch(1),
            },
            2,
        ),
    );
    apply(
        &mut core,
        envelope(&issuer, claim().build(&issuer, &mut ids()).unwrap(), 3),
    );
    let claim = ClaimId::from_u128(10);
    apply(
        &mut core,
        envelope(&issuer, Command::PostClaim { claim }, 4),
    );
    let receipt = ReceiptFence {
        receipt: ReceiptId::from_u128(20),
        epoch: 1,
    };
    apply(
        &mut core,
        envelope(
            &worker,
            Command::AcquireReceipt {
                claim,
                receipt: receipt.receipt,
                epoch: 1,
            },
            5,
        ),
    );
    apply(
        &mut core,
        envelope(
            &worker,
            Command::BeginEvidenceSet {
                claim,
                receipt,
                evidence_set: EvidenceSetId::from_u128(21),
            },
            6,
        ),
    );
    let artifact = ArtifactDocument {
        claim: id(10),
        receipt: ReceiptDocument {
            id: id(20),
            epoch: 1,
        },
        evidence_set: id(21),
        id: Some(id(22)),
        kind: "test-report".into(),
        schema_hash: "ab".repeat(32),
        metadata: vec![],
        payload: PayloadDocument::Text { text: "{}".into() },
        inputs: vec![],
        visibility: vec![],
    }
    .build(&worker, &mut ids())
    .unwrap();
    assert!(matches!(
        core.prepare(&envelope(&worker, artifact, 7)),
        Err(DomainOutcome::Refuse {
            code: ErrorCode::EvidenceNotDurable,
            ..
        })
    ));
    assert!(core.snapshot().artifacts.is_empty());
    let yaml = format!(
        "id: '{}'\nclaim: '{}'\nreceipt:\n  id: '{}'\n  epoch: 1\nevidence_set: '{}'\nmanifest: []\nsummary: Work interrupted before evidence was attached\nconfidence: tentative\noutcome: interrupted\n",
        id(23),
        id(10),
        id(20),
        id(21)
    );
    let testament: TestamentDocument = parse_document(yaml.as_bytes(), InputFormat::Yaml).unwrap();
    apply(
        &mut core,
        envelope(&worker, testament.build(&worker, &mut ids()).unwrap(), 8),
    );
    assert_eq!(
        core.snapshot().claims[&claim].lifecycle().status,
        ClaimStatus::TestamentGenerated
    );
    assert_eq!(
        core.snapshot().testaments[&TestamentId::from_u128(23)]
            .content()
            .outcome,
        OutcomeKind::Interrupted
    );
    assert!(core.snapshot().runs.is_empty());
}

#[test]
fn reserved_relations_invalid_artifact_kind_and_nested_unknown_fields_reject() {
    for kind in [
        "issuer",
        "subject",
        "claim_action",
        "caused_by",
        "evaluator",
        "invalidates",
        "contributed_by",
    ] {
        let mut document = claim();
        document.relations.push(ClaimRelationDocument {
            kind: kind.into(),
            target: id(9),
        });
        assert!(document.build(&context(), &mut ids()).is_err(), "{kind}");
    }
    let json = serde_json::to_string(&claim()).unwrap();
    let bad = json.replace(
        "\"description\":\"Receipt of the testament\"",
        "\"description\":\"Receipt of the testament\",\"status\":\"pass\"",
    );
    assert!(parse_document::<ClaimDocument>(bad.as_bytes(), InputFormat::Json).is_err());
    let bad_payload = br#"{"type":"text","text":"evidence","durable":true}"#;
    assert!(parse_document::<PayloadDocument>(bad_payload, InputFormat::Json).is_err());
    for kind in ["Test Report", "../Oops", "", "x:"] {
        let document = ArtifactDocument {
            claim: id(10),
            receipt: ReceiptDocument {
                id: id(20),
                epoch: 1,
            },
            evidence_set: id(21),
            id: None,
            kind: kind.into(),
            schema_hash: "ab".repeat(32),
            metadata: vec![],
            payload: PayloadDocument::Inline { bytes: vec![] },
            inputs: vec![],
            visibility: vec![],
        };
        assert!(document.build(&context(), &mut ids()).is_err());
    }
    assert!(
        PayloadDocument::Inline {
            bytes: vec![0; 16 * 1024 + 1]
        }
        .build()
        .is_err()
    );
}

#[test]
fn self_alias_resolves_identically_without_bypassing_self_targeting_rules() {
    let context = context();
    let mut aliases = claim();
    aliases.target = "self".into();
    aliases.validations[0].evaluator = "self".into();
    aliases.validations[0].contributed_by = vec!["self".into()];
    assert!(aliases.clone().build(&context, &mut ids()).is_err());
    aliases.action = "handoff".into();
    let mut exact = aliases.clone();
    exact.target = context.actor.to_string();
    exact.validations[0].evaluator = context.actor.to_string();
    exact.validations[0].contributed_by = vec![context.actor.to_string()];
    let json = serde_json::to_vec(&aliases).unwrap();
    let parsed: ClaimDocument = parse_document(&json, InputFormat::Json).unwrap();
    let yaml = format!(
        "id: '{}'\noccurrence: '{}'\ndescription: Check the report\ntarget: self\naction: handoff\nscopes: [{{kind: file, key: report.json}}]\nvalidations:\n  - id: '{}'\n    kind: receipt\n    phase: whole_work\n    mode: required\n    description: Receipt of the testament\n    evaluator: self\n    contributed_by: [self]\n    policy_revision: 1\n",
        id(10),
        id(12),
        id(11)
    );
    let yaml: ClaimDocument = parse_document(yaml.as_bytes(), InputFormat::Yaml).unwrap();
    let expected = exact.build(&context, &mut ids()).unwrap();
    for document in [aliases, parsed, yaml] {
        assert_eq!(document.build(&context, &mut ids()).unwrap(), expected);
    }
    let mut core = Core::new(context.ledger, Limits::default());
    apply(
        &mut core,
        envelope(
            &context,
            Command::NegotiateEpoch {
                epoch: RequestEpoch(1),
            },
            1,
        ),
    );
    apply(&mut core, envelope(&context, expected, 2));
    for name in ["SELF", " self", "other", "runtime"] {
        assert!(resolve_participant(name, &context).is_err());
    }
    assert!(parse_id("self").is_err());
    assert!(
        resolve_participant(
            "self",
            &BuildContext {
                actor: ParticipantId::from_u128(0),
                ..context
            }
        )
        .is_err()
    );
}
