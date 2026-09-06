use super::*;
use std::fmt::Debug;

fn rows<T>(bytes: &[u8], expected_count: usize) -> Vec<T>
where
    T: V1 + Serialize + for<'de> Deserialize<'de> + Debug + PartialEq,
{
    let (Value(frozen), rest): (Value<Vec<T>>, _) = postcard::take_from_bytes(bytes).unwrap();
    assert!(rest.is_empty());
    assert_eq!(frozen.len(), expected_count);
    let original: Vec<T> = postcard::from_bytes(bytes).unwrap();
    assert_eq!(frozen, original);
    assert_eq!(postcard::to_stdvec(&Ref(&frozen)).unwrap(), bytes);
    for value in &frozen {
        let original = postcard::to_stdvec(value).unwrap();
        let encoded = postcard::to_stdvec(&Ref(value)).unwrap();
        assert_eq!(encoded, original);
        let (Value(decoded), rest): (Value<T>, _) = postcard::take_from_bytes(&encoded).unwrap();
        assert!(rest.is_empty());
        assert_eq!(&decoded, value);
    }
    frozen
}

#[test]
fn all_twelve_native_types_match_the_original_writer_corpus() {
    rows::<ConsumerId>(
        include_bytes!("../fixtures/durable-v1/consumer-ids.rows"),
        4,
    );
    rows::<ConsumerKey>(
        include_bytes!("../fixtures/durable-v1/consumer-keys.rows"),
        4,
    );
    rows::<PositionOffset>(
        include_bytes!("../fixtures/durable-v1/position-offsets.rows"),
        5,
    );
    rows::<Position>(include_bytes!("../fixtures/durable-v1/positions.rows"), 50);
    rows::<CursorToken>(
        include_bytes!("../fixtures/durable-v1/cursor-tokens.rows"),
        50,
    );
    rows::<DeltaFilter>(
        include_bytes!("../fixtures/durable-v1/delta-filters.rows"),
        4,
    );
    rows::<ResyncReason>(
        include_bytes!("../fixtures/durable-v1/resync-reasons.rows"),
        5,
    );
    rows::<CursorMode>(
        include_bytes!("../fixtures/durable-v1/cursor-modes.rows"),
        9,
    );
    rows::<CursorRecord>(
        include_bytes!("../fixtures/durable-v1/cursor-records.rows"),
        36,
    );
    rows::<CursorCheckpoint>(
        include_bytes!("../fixtures/durable-v1/cursor-checkpoints.rows"),
        3,
    );
    rows::<CursorCommand>(
        include_bytes!("../fixtures/durable-v1/cursor-commands.rows"),
        18,
    );
    rows::<CursorOperation>(
        include_bytes!("../fixtures/durable-v1/cursor-operations.rows"),
        18,
    );
}

const OPERATIONS: [&[u8]; 18] = [
    include_bytes!("../fixtures/durable-v1/operation-00.bin"),
    include_bytes!("../fixtures/durable-v1/operation-01.bin"),
    include_bytes!("../fixtures/durable-v1/operation-02.bin"),
    include_bytes!("../fixtures/durable-v1/operation-03.bin"),
    include_bytes!("../fixtures/durable-v1/operation-04.bin"),
    include_bytes!("../fixtures/durable-v1/operation-05.bin"),
    include_bytes!("../fixtures/durable-v1/operation-06.bin"),
    include_bytes!("../fixtures/durable-v1/operation-07.bin"),
    include_bytes!("../fixtures/durable-v1/operation-08.bin"),
    include_bytes!("../fixtures/durable-v1/operation-09.bin"),
    include_bytes!("../fixtures/durable-v1/operation-10.bin"),
    include_bytes!("../fixtures/durable-v1/operation-11.bin"),
    include_bytes!("../fixtures/durable-v1/operation-12.bin"),
    include_bytes!("../fixtures/durable-v1/operation-13.bin"),
    include_bytes!("../fixtures/durable-v1/operation-14.bin"),
    include_bytes!("../fixtures/durable-v1/operation-15.bin"),
    include_bytes!("../fixtures/durable-v1/operation-16.bin"),
    include_bytes!("../fixtures/durable-v1/operation-17.bin"),
];

#[test]
fn all_nine_operation_ordinals_and_original_fields_are_fixed() {
    for (index, bytes) in OPERATIONS.into_iter().enumerate() {
        let (ordinal, _): (u32, _) = postcard::take_from_bytes(bytes).unwrap();
        assert_eq!(ordinal as usize, index % 9);
        let (Value(operation), rest): (Value<CursorOperation>, _) =
            postcard::take_from_bytes(bytes).unwrap();
        assert!(rest.is_empty());
        assert_eq!(postcard::to_stdvec(&Ref(&operation)).unwrap(), bytes);
        for length in 0..bytes.len() {
            assert!(
                postcard::take_from_bytes::<Value<CursorOperation>>(&bytes[..length]).is_err(),
                "operation {index}, truncation {length}"
            );
        }
    }
}

#[test]
fn scalar_enum_vectors_do_not_depend_on_live_discriminant_encoding() {
    assert_eq!(
        postcard::to_stdvec(&Ref(&ConsumerId([0; 16]))).unwrap(),
        [0; 16]
    );
    assert_eq!(
        postcard::to_stdvec(&Ref(&PositionOffset::Delta(128))).unwrap(),
        [0, 128, 1]
    );
    assert_eq!(
        postcard::to_stdvec(&Ref(&PositionOffset::Resolved)).unwrap(),
        [1]
    );
    assert_eq!(postcard::to_stdvec(&Ref(&DeltaFilter::All)).unwrap(), [0]);
    assert_eq!(
        postcard::to_stdvec(&Ref(&DeltaFilter::Claims(BTreeSet::new()))).unwrap(),
        [1, 0]
    );
    for (reason, ordinal) in [
        (ResyncReason::HistoryExpired, 0),
        (ResyncReason::LeaseExpired, 1),
        (ResyncReason::SlowConsumer, 2),
        (ResyncReason::SnapshotExpired, 3),
        (ResyncReason::ExplicitReset, 4),
    ] {
        assert_eq!(postcard::to_stdvec(&Ref(&reason)).unwrap(), [ordinal]);
    }
    assert_eq!(postcard::to_stdvec(&Ref(&CursorMode::Live)).unwrap(), [0]);
    assert_eq!(
        postcard::to_stdvec(&Ref(&CursorMode::Seeding {
            snapshot: SessionSeq(128)
        }))
        .unwrap(),
        [1, 128, 1]
    );
    assert_eq!(
        postcard::to_stdvec(&Ref(&CursorMode::Resync {
            reason: ResyncReason::ExplicitReset
        }))
        .unwrap(),
        [2, 4]
    );
    assert_eq!(
        postcard::to_stdvec(&Ref(&CursorMode::Protected)).unwrap(),
        [3]
    );
    assert_eq!(
        postcard::to_stdvec(&Ref(&CursorOperation::AdvanceFloor {
            through: SessionSeq(128)
        }))
        .unwrap(),
        [6, 128, 1]
    );
}

fn rejects_unknown<T: V1>(first_unknown: u32) {
    for ordinal in [first_unknown, 127, 128, u32::MAX] {
        let mut bytes = postcard::to_stdvec(&ordinal).unwrap();
        bytes.extend_from_slice(&[0; 256]);
        assert!(postcard::take_from_bytes::<Value<T>>(&bytes).is_err());
    }
}

#[test]
fn every_enum_rejects_unknown_ordinals_even_with_complete_looking_payload() {
    rejects_unknown::<PositionOffset>(2);
    rejects_unknown::<DeltaFilter>(2);
    rejects_unknown::<ResyncReason>(5);
    rejects_unknown::<CursorMode>(4);
    rejects_unknown::<CursorOperation>(9);
}

#[test]
fn original_set_sorting_deduplication_and_map_last_duplicate_wins_are_preserved() {
    let input = include_bytes!("../fixtures/durable-v1/unsorted-duplicate-filter.input");
    let (Value(filter), rest): (Value<DeltaFilter>, _) = postcard::take_from_bytes(input).unwrap();
    assert!(rest.is_empty());
    assert_eq!(
        filter,
        DeltaFilter::Claims([1, 3, 9].map(ClaimId::from_u128).into_iter().collect())
    );
    assert_eq!(
        postcard::to_stdvec(&Ref(&filter)).unwrap(),
        include_bytes!("../fixtures/durable-v1/unsorted-duplicate-filter.normalized")
    );

    let input = include_bytes!("../fixtures/durable-v1/unsorted-duplicate-checkpoint.input");
    let (Value(checkpoint), rest): (Value<CursorCheckpoint>, _) =
        postcard::take_from_bytes(input).unwrap();
    assert!(rest.is_empty());
    assert_eq!(checkpoint.consumers.len(), 2);
    assert_eq!(
        checkpoint.consumers.keys().copied().collect::<Vec<_>>(),
        [ConsumerId::from_u128(1), ConsumerId::from_u128(u128::MAX)]
    );
    assert_eq!(
        checkpoint
            .consumers
            .get(&ConsumerId::from_u128(u128::MAX))
            .unwrap()
            .filter,
        DeltaFilter::Claims(BTreeSet::new())
    );
    assert_eq!(
        postcard::to_stdvec(&Ref(&checkpoint)).unwrap(),
        include_bytes!("../fixtures/durable-v1/unsorted-duplicate-checkpoint.normalized")
    );
}

#[test]
fn decoders_preserve_serializable_values_outside_current_admission_rules() {
    let positions = rows::<Position>(include_bytes!("../fixtures/durable-v1/positions.rows"), 50);
    assert_eq!(positions[0].sequence, SessionSeq(0));
    assert_eq!(positions[0].offset, PositionOffset::Delta(0));
    assert!(
        positions[0]
            .validate(positions[0].ledger, SessionSeq(u64::MAX))
            .is_err()
    );
    let tokens = rows::<CursorToken>(
        include_bytes!("../fixtures/durable-v1/cursor-tokens.rows"),
        50,
    );
    assert_eq!(tokens[0].generation, 0);
    assert_ne!(tokens[1].key.ledger, tokens[1].position.ledger);
    assert!(tokens[1].same_stream(tokens[1]).is_err());
    let (Value(checkpoint), rest): (Value<CursorCheckpoint>, _) =
        postcard::take_from_bytes(include_bytes!("../fixtures/durable-v1/checkpoint-02.bin"))
            .unwrap();
    assert!(rest.is_empty());
    assert_eq!(checkpoint.schema, u16::MAX);
    assert_eq!(checkpoint.revision, u64::MAX);
    assert_eq!(checkpoint.clock, u64::MAX);
    assert_eq!(checkpoint.floor, SessionSeq(u64::MAX));
    assert_eq!(checkpoint.consumers.len(), 36);
}

#[test]
fn hostile_collection_counts_cannot_preallocate_from_an_untrusted_hint() {
    let count = postcard::to_stdvec(&usize::MAX).unwrap();
    let mut filter = vec![1];
    filter.extend_from_slice(&count);
    assert!(postcard::take_from_bytes::<Value<DeltaFilter>>(&filter).is_err());
    let mut checkpoint = include_bytes!("../fixtures/durable-v1/checkpoint-00.bin").to_vec();
    // Empty checkpoint's final byte is the consumers map count.
    assert_eq!(checkpoint.pop(), Some(0));
    checkpoint.extend_from_slice(&count);
    assert!(postcard::take_from_bytes::<Value<CursorCheckpoint>>(&checkpoint).is_err());
    // A Register filter still reaches the same no-hint set decoder when nested.
    let mut operation = vec![0];
    operation.extend_from_slice(&[0; 16 + 32]);
    operation.extend_from_slice(&filter);
    assert!(postcard::take_from_bytes::<Value<CursorOperation>>(&operation).is_err());
}

#[test]
fn checkpoint_truncation_and_enclosing_exact_body_boundary_are_distinct() {
    for bytes in [
        include_bytes!("../fixtures/durable-v1/checkpoint-00.bin").as_slice(),
        include_bytes!("../fixtures/durable-v1/checkpoint-01.bin").as_slice(),
        include_bytes!("../fixtures/durable-v1/checkpoint-02.bin").as_slice(),
    ] {
        for length in [0, 1, 2, 3, bytes.len() / 2, bytes.len() - 1] {
            assert!(
                postcard::take_from_bytes::<Value<CursorCheckpoint>>(&bytes[..length]).is_err()
            );
        }
        let mut framed = bytes.to_vec();
        framed.extend_from_slice(&[0xee, 0xff]);
        let (Value(checkpoint), rest): (Value<CursorCheckpoint>, _) =
            postcard::take_from_bytes(&framed).unwrap();
        assert_eq!(rest, [0xee, 0xff]);
        assert_eq!(postcard::to_stdvec(&Ref(&checkpoint)).unwrap(), bytes);
        // The owning Session reader must reject this remainder before publication.
        assert!(!rest.is_empty());
    }
}
