use super::*;
use serde::de::DeserializeOwned;
use std::fmt::Debug;

// Fixed Postcard V1 vectors, written from the original field/variant layout.
// They cover every content field, ordered/repeated manifests, both handler
// kinds, Unicode/NUL strings, option tags and multi-byte integer encodings.
const CLAIM: &str = concat!(
    "1111111111111111111111111111111122222222222222222222222222222222ac023333333333333333333333333333",
    "3333046300c3a90401004444444444444444444444444444444404020306011111111111111111111111111111111122",
    "222222222222222222222222222222015555555555555555555555555555555508036666666666666666666666666666",
    "6666020101610502ceb20277777777777777777777777777777777888888888888888888888888888888888888888888",
    "888888888888888888888899999999999999999999999999999999aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "aaaaaaaaaaaaaaaaaaaaaa01bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb8001ffffffffffffffffff01",
);
const VALIDATION: &str = concat!(
    "111111111111111111111111111111112222222222222222222222222222222202333333333333333333333333333333",
    "3303020107696e7370656374010171444444444444444444444444444444440255555555555555555555555555555555",
    "666666666666666666666666666666666666666666666666666666666666666600777777777777777777777777777777",
    "778888888888888888888888888888888888888888888888888888888888888888010200000000000000000000000000",
    "000000000000000000000000000000000000009999999999999999999999999999999999999999999999999999999999",
    "999999021111111111111111111111111111111122222222222222222222222222222222808001",
);
const ARTIFACT: &str = concat!(
    "1111111111111111111111111111111122222222222222222222222222222222ffff0300aaaaaaaaaaaaaaaaaaaaaaaa",
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0500017f80ff01bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbcccccccccc",
    "ccccccccccccccccccccccccccccccccccccccccccccccccccccccac0203dddddddddddddddddddddddddddddddd0100",
    "000000000000000000000000000000000211111111111111111111111111111111222222222222222222222222222222",
    "220244444444444444444444444444444444111111111111111111111111111111112222222222222222222222222222",
    "222204333333333333333333333333333333330200047465616d",
);
const TESTAMENT: &str = concat!(
    "111111111111111111111111111111112222222222222222222222222222222201333333333333333333333333333333",
    "334444444444444444444444444444444481015555555555555555555555555555555503666666666666666666666666",
    "666666667777777777777777777777777777777777777777777777777777777777777777888888888888888888888888",
    "888888889999999999999999999999999999999999999999999999999999999999999999666666666666666666666666",
    "66666666777777777777777777777777777777777777777777777777777777777777777704646f6e650403",
);

fn hex(value: &str) -> Vec<u8> {
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|bytes| u8::from_str_radix(std::str::from_utf8(bytes).unwrap(), 16).unwrap())
        .collect()
}
fn check<T>(value: &T, expected: &[u8])
where
    T: V1 + Serialize + DeserializeOwned + PartialEq + Debug,
{
    assert_eq!(
        postcard::to_stdvec(value).unwrap(),
        expected,
        "original writer"
    );
    assert_eq!(
        postcard::to_stdvec(&Ref(value)).unwrap(),
        expected,
        "frozen writer"
    );
    let (Value(decoded), rest): (Value<T>, _) = postcard::take_from_bytes(expected).unwrap();
    assert!(rest.is_empty());
    assert_eq!(&decoded, value);
    assert_eq!(
        &postcard::from_bytes::<T>(expected).unwrap(),
        value,
        "original reader"
    );
    for length in 0..expected.len() {
        assert!(postcard::take_from_bytes::<Value<T>>(&expected[..length]).is_err());
    }
}
fn parity<T>(value: &T)
where
    T: V1 + Serialize + DeserializeOwned + PartialEq + Debug,
{
    check(value, &postcard::to_stdvec(value).unwrap());
}
fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId([0x11; 16]),
        session: SessionId([0x22; 16]),
    }
}
fn reference(kind: ObjectKind, byte: u8) -> ObjectRef {
    ObjectRef {
        ledger: ledger(),
        kind,
        id: ObjectId([byte; 16]),
    }
}
fn claim() -> ClaimContent {
    ClaimContent {
        ledger: ledger(),
        schema: 300,
        occurrence: OccurrenceId([0x33; 16]),
        description: "c\0é".into(),
        relations: BTreeSet::from([
            Relation {
                kind: RelationKind::Issuer,
                target: RelationTarget::Participant(ParticipantId([0x44; 16])),
            },
            Relation {
                kind: RelationKind::ClaimAction,
                target: RelationTarget::Action(ActionType::Challenge),
            },
            Relation {
                kind: RelationKind::DependsOn,
                target: RelationTarget::Object(reference(ObjectKind::Claim, 0x55)),
            },
            Relation {
                kind: RelationKind::CausedBy,
                target: RelationTarget::Root(RootCommandId([0x66; 16])),
            },
        ]),
        scopes: BTreeSet::from([
            Scope {
                kind: ScopeKind::File,
                key: "a".into(),
            },
            Scope {
                kind: ScopeKind::Component,
                key: "β".into(),
            },
        ]),
        requirements: vec![
            RequirementRef {
                id: ValidationId([0x77; 16]),
                specification: ContentHash([0x88; 32]),
            },
            RequirementRef {
                id: ValidationId([0x99; 16]),
                specification: ContentHash([0xaa; 32]),
            },
        ],
        deadline: Some(Deadline {
            timer: TimerId([0xbb; 16]),
            generation: 128,
            at: u64::MAX,
        }),
    }
}
fn validation() -> ValidationContent {
    ValidationContent {
        ledger: ledger(),
        schema: 2,
        claim: ClaimId([0x33; 16]),
        kind: ValidationKind::Inspection,
        phase: ValidationPhase::Increment,
        mode: ValidationMode::Observe,
        description: "inspect".into(),
        quality_bar: Some("q".into()),
        evaluator: ParticipantId([0x44; 16]),
        handlers: vec![
            HandlerRef {
                id: ValidatorId([0x55; 16]),
                version: ContentHash([0x66; 32]),
                agentic: false,
            },
            HandlerRef {
                id: ValidatorId([0x77; 16]),
                version: ContentHash([0x88; 32]),
                agentic: true,
            },
        ],
        evidence_schemas: BTreeSet::from([ContentHash([0; 32]), ContentHash([0x99; 32])]),
        contributed_by: BTreeSet::from([ParticipantId([0x11; 16]), ParticipantId([0x22; 16])]),
        policy_revision: 16_384,
    }
}
fn artifact() -> ArtifactContent {
    ArtifactContent {
        ledger: ledger(),
        schema: u16::MAX,
        kind: String::new(),
        schema_hash: ContentHash([0xaa; 32]),
        metadata: vec![0, 1, 127, 128, 255],
        payload: ArtifactPayload::Content(ContentRef {
            domain: ContentDomainId([0xbb; 16]),
            root: ContentHash([0xcc; 32]),
            length: 300,
            class: ContentClass::Checkpoint,
        }),
        producer: ParticipantId([0xdd; 16]),
        receipt: Some(ReceiptFence {
            receipt: ReceiptId::default(),
            epoch: 0,
        }),
        inputs: BTreeSet::from([
            reference(ObjectKind::Testament, 0x44),
            reference(ObjectKind::Artifact, 0x33),
        ]),
        visibility: BTreeSet::from([String::new(), "team".into()]),
    }
}
fn testament() -> TestamentContent {
    let first = ArtifactRef {
        id: ArtifactId([0x66; 16]),
        hash: ContentHash([0x77; 32]),
    };
    TestamentContent {
        ledger: ledger(),
        schema: 1,
        claim: ClaimId([0x33; 16]),
        receipt: ReceiptFence {
            receipt: ReceiptId([0x44; 16]),
            epoch: 129,
        },
        evidence_set: EvidenceSetId([0x55; 16]),
        artifacts: vec![
            first,
            ArtifactRef {
                id: ArtifactId([0x88; 16]),
                hash: ContentHash([0x99; 32]),
            },
            first,
        ],
        summary: "done".into(),
        confidence: Confidence::Consensus,
        outcome: OutcomeKind::Refused,
    }
}

#[test]
fn content_field_order_matches_fixed_original_vectors() {
    check(&claim(), &hex(CLAIM));
    check(&validation(), &hex(VALIDATION));
    check(&artifact(), &hex(ARTIFACT));
    check(&testament(), &hex(TESTAMENT));
}

#[test]
fn enum_ordinals_and_all_payload_shapes_remain_frozen() {
    check(
        &RelationTarget::Participant(ParticipantId([0x11; 16])),
        &[&[0][..], &[0x11; 16]].concat(),
    );
    check(
        &RelationTarget::Object(reference(ObjectKind::Artifact, 0x33)),
        &[&[1][..], &[0x11; 16], &[0x22; 16], &[4], &[0x33; 16]].concat(),
    );
    check(&RelationTarget::Action(ActionType::Challenge), &[2, 3]);
    check(
        &RelationTarget::Root(RootCommandId([0x44; 16])),
        &[&[3][..], &[0x44; 16]].concat(),
    );
    check(
        &ArtifactPayload::Inline(vec![0, 127, 128, 255]),
        &[0, 4, 0, 127, 128, 255],
    );
    check(&ArtifactPayload::Inline(Vec::new()), &[0, 0]);
    check(
        &ArtifactPayload::Content(ContentRef {
            domain: ContentDomainId([0x55; 16]),
            root: ContentHash([0x66; 32]),
            length: 300,
            class: ContentClass::Evidence,
        }),
        &[&[1][..], &[0x55; 16], &[0x66; 32], &[0xac, 2, 2]].concat(),
    );
    assert!(postcard::take_from_bytes::<Value<RelationTarget>>(&[4]).is_err());
    assert!(postcard::take_from_bytes::<Value<ArtifactPayload>>(&[2]).is_err());
}

#[test]
fn stored_families_preserve_opaque_hashes_and_independent_lifecycle_fields() {
    parity(&StoredObject::new(
        claim(),
        ContentHash([0xff; 32]),
        ClaimLifecycle {
            status: ClaimStatus::Generated,
            revision: ObjectRevision(0),
            created: SessionSeq(0),
            history: vec![StatusFact {
                status: ClaimStatus::Satisfied,
                sequence: SessionSeq(u64::MAX),
            }],
            receipt: Some(Receipt {
                fence: ReceiptFence {
                    receipt: ReceiptId::default(),
                    epoch: 0,
                },
                holder: ParticipantId::default(),
                acquired: SessionSeq(9),
            }),
            evidence_set: Some(EvidenceSetId::default()),
            testament: Some(TestamentId::default()),
            local_complete: true,
            released: false,
            terminal_witness: Some(ClaimId::default()),
        },
    ));
    parity(&StoredObject::new(
        validation(),
        ContentHash([0; 32]),
        ValidationLifecycle {
            created: SessionSeq(u64::MAX),
            latest_epoch: 0,
        },
    ));
    parity(&StoredObject::new(
        artifact(),
        ContentHash([0x12; 32]),
        ArtifactLifecycle {
            created: SessionSeq(0),
            custody_revision: u64::MAX,
        },
    ));
    parity(&StoredObject::new(
        testament(),
        ContentHash([0x34; 32]),
        TestamentLifecycle {
            created: SessionSeq(127),
            acknowledged: Some(SessionSeq(0)),
        },
    ));
    let mut sparse_claim = claim();
    sparse_claim.description.clear();
    sparse_claim.relations.clear();
    sparse_claim.scopes.clear();
    sparse_claim.requirements.clear();
    sparse_claim.deadline = None;
    parity(&sparse_claim);
    let mut sparse_validation = validation();
    sparse_validation.quality_bar = None;
    sparse_validation.handlers.clear();
    sparse_validation.evidence_schemas.clear();
    sparse_validation.contributed_by.clear();
    parity(&sparse_validation);
    let mut inline = artifact();
    inline.payload = ArtifactPayload::Inline(Vec::new());
    inline.receipt = None;
    inline.inputs.clear();
    inline.visibility.clear();
    parity(&inline);
}

// This type deliberately implements neither Clone nor live Serde. The generic
// stored wrapper must invoke the frozen codec and borrow/move its allocations.
struct FrozenOnly(Vec<u8>);
impl V1 for FrozenOnly {
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize_v1(serializer)
    }
    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Vec::<u8>::deserialize_v1(deserializer).map(Self)
    }
}
#[test]
fn stored_wrapper_needs_neither_clone_nor_live_serde_for_its_fields() {
    let object = StoredObject::new(
        FrozenOnly(vec![1, 2, 3]),
        ContentHash([0xfe; 32]),
        FrozenOnly(vec![4, 5]),
    );
    let allocation = object.content().0.as_ptr();
    let bytes = postcard::to_stdvec(&Ref(&object)).unwrap();
    assert_eq!(bytes, [&[3, 1, 2, 3][..], &[0xfe; 32], &[2, 4, 5]].concat());
    assert_eq!(object.content().0.as_ptr(), allocation);
    let (Value(decoded), rest): (Value<StoredObject<FrozenOnly, FrozenOnly>>, _) =
        postcard::take_from_bytes(&bytes).unwrap();
    assert!(rest.is_empty());
    assert_eq!(decoded.content().0, [1, 2, 3]);
    assert_eq!(decoded.content_hash(), ContentHash([0xfe; 32]));
    assert_eq!(decoded.lifecycle().0, [4, 5]);
}
