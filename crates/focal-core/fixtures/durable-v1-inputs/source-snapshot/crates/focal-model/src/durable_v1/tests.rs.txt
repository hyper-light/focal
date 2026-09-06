use super::*;
use crate::*;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;

fn parity<T: V1 + Serialize + PartialEq + Debug>(value: &T) {
    let original = postcard::to_stdvec(value).unwrap();
    let frozen = postcard::to_stdvec(&Ref(value)).unwrap();
    assert_eq!(frozen, original);
    let (Value(decoded), rest) = postcard::take_from_bytes::<Value<T>>(&original).unwrap();
    assert!(rest.is_empty());
    assert_eq!(&decoded, value);
}

#[test]
fn numeric_vocabularies_preserve_every_original_code_and_reject_unknown_values() {
    macro_rules! vocabulary {
        ($($name:ident),+ $(,)?) => {$ (
            for value in $name::ALL {
                parity(value);
                assert_eq!(postcard::to_stdvec(&Ref(value)).unwrap(), postcard::to_stdvec(&value.code()).unwrap());
            }
            for code in [0u16, u16::MAX] {
                let bytes = postcard::to_stdvec(&code).unwrap();
                assert!(postcard::from_bytes::<Value<$name>>(&bytes).is_err());
            }
        )+};
    }
    vocabulary!(
        ObjectKind,
        ClaimStatus,
        ActionType,
        ScopeKind,
        RelationKind,
        ValidationKind,
        ValidationPhase,
        ValidationMode,
        VerdictValue,
        Confidence,
        OutcomeKind,
        ContentClass,
        Disposition,
        ErrorCode,
        LifecycleAction
    );
}

#[test]
fn fixed_id_hash_counter_and_reference_fields_keep_their_original_width_and_order() {
    macro_rules! ids {
        ($($name:ident),+ $(,)?) => {$ (
            for raw in [0u128, 128, u128::MAX] {
                let value = $name::from_u128(raw);
                parity(&value);
                assert_eq!(postcard::to_stdvec(&Ref(&value)).unwrap(), raw.to_be_bytes());
            }
        )+};
    }
    ids!(
        TenantId,
        SessionId,
        ObjectId,
        ClaimId,
        TestamentId,
        ValidationId,
        ArtifactId,
        ParticipantId,
        RequestId,
        ReceiptId,
        EvidenceSetId,
        OccurrenceId,
        RootCommandId,
        MonitorId,
        ValidatorId,
        TimerId,
        ContentDomainId
    );
    macro_rules! counters {
        ($($name:ident),+ $(,)?) => {$ (
            for raw in [0u64, 127, 128, u64::MAX] {
                parity(&$name(raw));
                assert_eq!(postcard::to_stdvec(&Ref(&$name(raw))).unwrap(), postcard::to_stdvec(&raw).unwrap());
            }
        )+};
    }
    counters!(
        SessionSeq,
        RequestEpoch,
        ObjectRevision,
        RouteEpoch,
        RaftIndex,
        RaftTerm
    );
    let hash = ContentHash([0xa5; 32]);
    parity(&hash);
    assert_eq!(postcard::to_stdvec(&Ref(&hash)).unwrap(), [0xa5; 32]);
    let ledger = LedgerId {
        tenant: TenantId([1; 16]),
        session: SessionId([2; 16]),
    };
    parity(&ledger);
    for kind in ObjectKind::ALL {
        parity(&ObjectRef {
            ledger,
            kind: *kind,
            id: ObjectId([3; 16]),
        });
    }
}

// Neither Clone nor current-model Serialize/Deserialize is implemented. The
// nested container path must use the historical codec and move its values.
#[derive(Debug, PartialEq, Eq)]
struct OnlyV1(u64);
impl V1 for OnlyV1 {
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u64(self.0)
    }
    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        u64::deserialize(deserializer).map(Self)
    }
}

#[test]
fn nested_collections_use_v1_without_live_serde_or_clone_and_preserve_duplicate_semantics() {
    let source = BTreeMap::from([(String::from("one"), vec![Some(7u64), None, Some(9)])]);
    let bytes = postcard::to_stdvec(&source).unwrap();
    let Value(decoded) =
        postcard::from_bytes::<Value<BTreeMap<String, Vec<Option<OnlyV1>>>>>(&bytes).unwrap();
    assert_eq!(decoded["one"], vec![Some(OnlyV1(7)), None, Some(OnlyV1(9))]);
    assert_eq!(postcard::to_stdvec(&Ref(&decoded)).unwrap(), bytes);

    // Postcard map entries and a sequence of pairs have the same raw layout.
    let duplicate = postcard::to_stdvec(&vec![(2u64, 3u64), (1, 4), (2, 5)]).unwrap();
    let original: BTreeMap<u64, u64> = postcard::from_bytes(&duplicate).unwrap();
    let Value(frozen): Value<BTreeMap<u64, u64>> = postcard::from_bytes(&duplicate).unwrap();
    assert_eq!(frozen, original);
    assert_eq!(frozen[&2], 5);
    let duplicate = postcard::to_stdvec(&vec![2u64, 1, 2]).unwrap();
    let original: BTreeSet<u64> = postcard::from_bytes(&duplicate).unwrap();
    let Value(frozen): Value<BTreeSet<u64>> = postcard::from_bytes(&duplicate).unwrap();
    assert_eq!(frozen, original);
    parity(&(ObjectKind::Artifact, ContentHash([17; 32])));
}

#[test]
fn collection_counts_do_not_authorize_unbounded_preallocation() {
    struct FalseCount;
    impl Iterator for FalseCount {
        type Item = u64;
        fn next(&mut self) -> Option<Self::Item> {
            None
        }
        fn size_hint(&self) -> (usize, Option<usize>) {
            (usize::MAX, Some(usize::MAX))
        }
    }
    let decoder = serde::de::value::SeqDeserializer::<_, serde::de::value::Error>::new(FalseCount);
    let Value(values) = Value::<Vec<u64>>::deserialize(decoder).unwrap();
    assert_eq!(values.capacity(), 0);
    let count = postcard::to_stdvec(&usize::MAX).unwrap();
    assert!(postcard::from_bytes::<Value<Vec<u64>>>(&count).is_err());
    assert!(postcard::from_bytes::<Value<BTreeSet<u64>>>(&count).is_err());
    assert!(postcard::from_bytes::<Value<BTreeMap<u64, u64>>>(&count).is_err());
}
