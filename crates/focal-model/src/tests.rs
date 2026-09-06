use crate::*;
use std::collections::BTreeSet;

fn fixtures() -> Vec<(&'static str, Vec<u8>, ContentHash)> {
    let ledger = LedgerId {
        tenant: TenantId::from_u128(1),
        session: SessionId::from_u128(2),
    };
    let validation = ValidationContent {
        ledger,
        schema: 1,
        claim: ClaimId::from_u128(3),
        kind: ValidationKind::Receipt,
        phase: ValidationPhase::WholeWork,
        mode: ValidationMode::Required,
        description: "receipt".into(),
        quality_bar: None,
        evaluator: ParticipantId::from_u128(4),
        handlers: vec![],
        evidence_schemas: BTreeSet::new(),
        contributed_by: BTreeSet::from([ParticipantId::from_u128(5)]),
        policy_revision: 1,
    };
    let claim = ClaimContent {
        ledger,
        schema: 1,
        occurrence: OccurrenceId::from_u128(6),
        description: "claim".into(),
        relations: BTreeSet::from([
            Relation {
                kind: RelationKind::Issuer,
                target: RelationTarget::Participant(ParticipantId::from_u128(5)),
            },
            Relation {
                kind: RelationKind::Subject,
                target: RelationTarget::Participant(ParticipantId::from_u128(4)),
            },
            Relation {
                kind: RelationKind::ClaimAction,
                target: RelationTarget::Action(ActionType::Work),
            },
            Relation {
                kind: RelationKind::CausedBy,
                target: RelationTarget::Root(RootCommandId::from_u128(7)),
            },
        ]),
        scopes: BTreeSet::from([Scope {
            kind: ScopeKind::File,
            key: "src/lib.rs".into(),
        }]),
        requirements: vec![RequirementRef {
            id: ValidationId::from_u128(8),
            specification: validation.specification_hash().unwrap(),
        }],
        deadline: None,
    };
    let receipt = ReceiptFence {
        receipt: ReceiptId::from_u128(9),
        epoch: 1,
    };
    let artifact = ArtifactContent {
        ledger,
        schema: 1,
        kind: "document".into(),
        schema_hash: ContentHash([10; 32]),
        metadata: vec![],
        payload: ArtifactPayload::Inline(b"proof".to_vec()),
        producer: ParticipantId::from_u128(4),
        receipt: Some(receipt),
        inputs: BTreeSet::new(),
        visibility: BTreeSet::new(),
    };
    let testament = TestamentContent {
        ledger,
        schema: 1,
        claim: ClaimId::from_u128(3),
        receipt,
        evidence_set: EvidenceSetId::from_u128(11),
        artifacts: vec![ArtifactRef {
            id: ArtifactId::from_u128(12),
            hash: artifact.content_hash().unwrap(),
        }],
        summary: "done".into(),
        confidence: Confidence::Committed,
        outcome: OutcomeKind::Complete,
    };
    vec![
        (
            "claim",
            claim.canonical_bytes().unwrap(),
            claim.content_hash().unwrap(),
        ),
        (
            "validation",
            validation.canonical_bytes().unwrap(),
            validation.content_hash().unwrap(),
        ),
        (
            "artifact",
            artifact.canonical_bytes().unwrap(),
            artifact.content_hash().unwrap(),
        ),
        (
            "testament",
            testament.canonical_bytes().unwrap(),
            testament.content_hash().unwrap(),
        ),
    ]
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[test]
fn four_family_canonical_golden_vectors() {
    let expected: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/domain/canonical-v1.json")).unwrap();
    for (family, bytes, hash) in fixtures() {
        assert_eq!(
            hex(&bytes),
            expected[family]["bytes"].as_str().unwrap(),
            "{family} bytes"
        );
        assert_eq!(
            hash.to_string(),
            expected[family]["hash"].as_str().unwrap(),
            "{family} hash"
        )
    }
}
#[test]
fn enums_are_numeric_append_only_and_unknown_codes_reject() {
    assert_eq!(ClaimStatus::ALL.len(), 20);
    assert_eq!(
        ClaimStatus::ALL.iter().filter(|s| s.is_terminal()).count(),
        13
    );
    for (index, status) in ClaimStatus::ALL.iter().enumerate() {
        assert_eq!(status.code(), index as u16 + 1);
        assert_eq!(
            postcard::to_allocvec(status).unwrap(),
            vec![index as u8 + 1]
        );
        assert_eq!(LifecycleAction::from(*status).code(), status.code());
    }
    assert_eq!(RelationKind::Invalidates.code(), 15);
    assert!(postcard::from_bytes::<ClaimStatus>(&[21]).is_err());
    assert!(postcard::from_bytes::<RelationKind>(&[16]).is_err());
}
#[test]
fn lazy_lifecycle_changes_do_not_change_authored_identity() {
    let (_, bytes, hash) = fixtures().remove(0);
    assert_eq!(ContentHash(*blake3::hash(&bytes).as_bytes()), hash);
    // Generic stored records serialize private lifecycle separately from canonical authored bytes.
    let stored = StoredObject::new("immutable".to_string(), hash, 1_u64);
    let next = stored.with_lifecycle(2);
    assert_eq!(stored.content_hash(), next.content_hash());
    assert_eq!(stored.content(), next.content());
    assert_ne!(stored.lifecycle(), next.lifecycle());
}
#[test]
fn identifier_types_have_stable_big_endian_addresses() {
    assert_eq!(ClaimId::from_u128(256).as_bytes()[14], 1);
    assert_eq!(ClaimId::from_u128(256).as_bytes()[15], 0);
    assert_ne!(
        TenantId::from_u128(1).to_string(),
        SessionId::from_u128(2).to_string()
    );
}

#[test]
fn canonical_length_overflow_never_returns_a_partial_identity() {
    let mut encoder = CanonicalEncoder::new();
    encoder.fixed(b"prefix");
    encoder.length(usize::MAX);
    encoder.string("later fields cannot erase an earlier encoding error");
    assert!(matches!(encoder.finish(), Err(CanonicalError::Length)));

    let mut boundary = CanonicalEncoder::new();
    boundary.length(u32::MAX as usize);
    assert_eq!(boundary.finish().unwrap(), u32::MAX.to_be_bytes());
}
