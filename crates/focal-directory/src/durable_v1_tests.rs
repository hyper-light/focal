use super::*;
use crate::{DirectoryError, placement_digest};
use serde::de::DeserializeOwned;
use std::fmt::Debug;

macro_rules! fixture {
    ($name:literal) => {
        include_bytes!(concat!("../fixtures/durable-v1/", $name))
    };
}

/// Rows whose live type gained fields after the writer was frozen: only the
/// V1 path reads the fixture, and it must write the identical bytes back.
fn frozen_parity<T: V1 + PartialEq + Debug>(bytes: &[u8]) -> T {
    let (Value(frozen), tail) = postcard::take_from_bytes::<Value<T>>(bytes).unwrap();
    assert!(tail.is_empty());
    assert_eq!(postcard::to_allocvec(&Ref(&frozen)).unwrap(), bytes);
    frozen
}

fn parity<T: V1 + DeserializeOwned + PartialEq + Debug>(bytes: &[u8]) -> T {
    let original: T = postcard::from_bytes(bytes).unwrap();
    let (Value(frozen), tail) = postcard::take_from_bytes::<Value<T>>(bytes).unwrap();
    assert!(tail.is_empty());
    assert_eq!(frozen, original);
    assert_eq!(postcard::to_allocvec(&Ref(&frozen)).unwrap(), bytes);
    frozen
}

#[test]
fn all_native_types_preserve_original_writer_bytes_and_owned_values() {
    parity::<Vec<ClusterId>>(fixture!("cluster-ids.rows"));
    parity::<Vec<RegionId>>(fixture!("region-ids.rows"));
    parity::<Vec<ZoneId>>(fixture!("zone-ids.rows"));
    parity::<Vec<PartitionId>>(fixture!("partition-ids.rows"));
    parity::<Vec<LogGroupId>>(fixture!("log-group-ids.rows"));
    parity::<Vec<OperationId>>(fixture!("operation-ids.rows"));
    parity::<Vec<WorkId>>(fixture!("work-ids.rows"));
    parity::<Vec<NamespaceKey>>(fixture!("namespace-keys.rows"));
    parity::<Vec<NamespaceRange>>(fixture!("namespace-ranges.rows"));
    parity::<Vec<NodeEnrollment>>(fixture!("node-enrollments.rows"));
    for load in frozen_parity::<Vec<NodeLoad>>(fixture!("node-loads.rows")) {
        assert_eq!(load.disk_available, 0);
    }
    for record in frozen_parity::<Vec<NodeRecord>>(fixture!("node-records.rows")) {
        assert!(record.load.is_none_or(|load| load.disk_available == 0));
    }
    parity::<Vec<FailureClass>>(fixture!("failure-classes.rows"));
    parity::<Vec<DurabilityIntent>>(fixture!("durability-intents.rows"));
    parity::<Vec<PlacementPolicy>>(fixture!("placement-policies.rows"));
    parity::<Vec<Placement>>(fixture!("placements.rows"));
    parity::<Vec<PlacementSpec>>(fixture!("placement-specs.rows"));
    parity::<Vec<SessionFenceKind>>(fixture!("session-fence-kinds.rows"));
    parity::<Vec<SessionFence>>(fixture!("session-fences.rows"));
    parity::<Vec<ReplicaReady>>(fixture!("replica-ready.rows"));
    parity::<Vec<DelegationFence>>(fixture!("delegation-fences.rows"));
}

#[test]
fn distinct_field_sentinels_pin_order_even_between_same_typed_fields() {
    let load = frozen_parity::<NodeLoad>(fixture!("distinct-node-load.bin"));
    assert_eq!(
        (
            load.node,
            load.generation,
            load.report,
            load.available_memory,
            load.active_weight
        ),
        (11, 12, 13, 14, 15)
    );
    let enrollment = parity::<NodeEnrollment>(fixture!("distinct-node-enrollment.bin"));
    assert_eq!(
        (
            enrollment.node,
            enrollment.generation,
            enrollment.authority_epoch
        ),
        (11, 12, 17)
    );
    let fence = parity::<SessionFence>(fixture!("distinct-session-fence.bin"));
    assert_eq!(
        (
            fence.sequence.0,
            fence.index.0,
            fence.term.0,
            fence.from_route.0,
            fence.to_route.0,
            fence.membership_epoch,
            fence.placement_epoch
        ),
        (15, 16, 17, 18, 19, 20, 21)
    );
    let ready = parity::<ReplicaReady>(fixture!("distinct-replica-ready.bin"));
    assert_eq!(
        (
            ready.route_epoch.0,
            ready.node,
            ready.node_generation,
            ready.through.0
        ),
        (14, 15, 16, 17)
    );
    let delegation = parity::<DelegationFence>(fixture!("distinct-delegation-fence.bin"));
    assert_eq!(
        (
            delegation.from_epoch,
            delegation.to_epoch,
            delegation.sealed_revision
        ),
        (19, 20, 21)
    );
}

#[test]
fn enums_and_fixed_width_identity_shapes_keep_original_ordinals() {
    assert_eq!(fixture!("failure-classes.rows"), &[3, 0, 1, 2]);
    assert_eq!(fixture!("session-fence-kinds.rows"), &[3, 0, 1, 2]);
    for (ordinal, value) in [FailureClass::Node, FailureClass::Zone, FailureClass::Region]
        .iter()
        .enumerate()
    {
        assert_eq!(
            postcard::to_allocvec(&Ref(value)).unwrap(),
            vec![u8::try_from(ordinal).unwrap()]
        );
    }
    for (ordinal, value) in [
        SessionFenceKind::Created,
        SessionFenceKind::Cutover,
        SessionFenceKind::Activated,
    ]
    .iter()
    .enumerate()
    {
        assert_eq!(
            postcard::to_allocvec(&Ref(value)).unwrap(),
            vec![u8::try_from(ordinal).unwrap()]
        );
    }
    assert!(postcard::from_bytes::<Value<FailureClass>>(&[3]).is_err());
    assert!(postcard::from_bytes::<Value<SessionFenceKind>>(&[3]).is_err());
    assert_eq!(
        postcard::to_allocvec(&Ref(&RegionId::from_u128(128))).unwrap(),
        128u128.to_be_bytes()
    );
    assert_eq!(
        postcard::to_allocvec(&Ref(&NamespaceKey([128; 32]))).unwrap(),
        [128; 32]
    );
}

#[test]
fn collection_decoders_preserve_original_sort_dedup_and_last_duplicate_value() {
    let Value(placement) =
        postcard::from_bytes::<Value<Placement>>(fixture!("unsorted-duplicate-placement.input"))
            .unwrap();
    assert_eq!(
        placement
            .voters
            .iter()
            .map(|(key, value)| (*key, *value))
            .collect::<Vec<_>>(),
        vec![(1, 2), (9, 3)]
    );
    assert_eq!(placement.content_copies.get(&3), Some(&6));
    assert_eq!(
        postcard::to_allocvec(&Ref(&placement)).unwrap(),
        fixture!("unsorted-duplicate-placement.normalized")
    );
    let Value(policy) =
        postcard::from_bytes::<Value<PlacementPolicy>>(fixture!("unsorted-duplicate-policy.input"))
            .unwrap();
    assert_eq!(
        policy.residency.iter().copied().collect::<Vec<_>>(),
        vec![RegionId::from_u128(1), RegionId::from_u128(9)]
    );
    assert_eq!(
        postcard::to_allocvec(&Ref(&policy)).unwrap(),
        fixture!("unsorted-duplicate-policy.normalized")
    );
}

#[test]
fn decoding_preserves_historically_serializable_values_without_live_admission() {
    let Value(ranges) =
        postcard::from_bytes::<Value<Vec<NamespaceRange>>>(fixture!("namespace-ranges.rows"))
            .unwrap();
    assert!(ranges[2].validate().is_err());
    assert!(ranges[3].validate().is_err());
    let Value(specs) =
        postcard::from_bytes::<Value<Vec<PlacementSpec>>>(fixture!("placement-specs.rows"))
            .unwrap();
    assert!(
        specs
            .iter()
            .any(|spec| spec.policy.residency.contains(&RegionId::UNKNOWN))
    );
    assert!(specs.iter().any(|spec| spec.placement.voters.is_empty()));
    assert!(
        specs
            .iter()
            .any(|spec| spec.policy.durability.max_failures == u16::MAX)
    );
    let Value(fences) =
        postcard::from_bytes::<Value<Vec<SessionFence>>>(fixture!("session-fences.rows")).unwrap();
    assert_eq!(fences[0].index, RaftIndex(0));
    assert_eq!(fences[2].index, RaftIndex(u64::MAX));
    assert_eq!(fences[2].from_route, fences[2].to_route);
}

#[test]
fn placement_identity_matches_original_digests_and_exact_domain_preimage() {
    let specs = parity::<Vec<PlacementSpec>>(fixture!("placement-specs.rows"));
    let expected: Vec<ContentHash> =
        postcard::from_bytes(fixture!("placement-digests.rows")).unwrap();
    let individual: [&[u8]; 18] = [
        fixture!("placement-00.bin"),
        fixture!("placement-01.bin"),
        fixture!("placement-02.bin"),
        fixture!("placement-03.bin"),
        fixture!("placement-04.bin"),
        fixture!("placement-05.bin"),
        fixture!("placement-06.bin"),
        fixture!("placement-07.bin"),
        fixture!("placement-08.bin"),
        fixture!("placement-09.bin"),
        fixture!("placement-10.bin"),
        fixture!("placement-11.bin"),
        fixture!("placement-12.bin"),
        fixture!("placement-13.bin"),
        fixture!("placement-14.bin"),
        fixture!("placement-15.bin"),
        fixture!("placement-16.bin"),
        fixture!("placement-17.bin"),
    ];
    assert_eq!(specs.len(), expected.len());
    for ((spec, hash), original) in specs.iter().zip(expected).zip(individual) {
        assert_eq!(postcard::to_allocvec(&Ref(spec)).unwrap(), original);
        assert_eq!(placement_digest(spec).unwrap(), hash);
        let mut preimage = b"focal:placement:v1\0".to_vec();
        preimage.extend_from_slice(original);
        assert_eq!(ContentHash(*blake3::hash(&preimage).as_bytes()), hash);
    }
}

#[test]
fn digest_retains_original_one_mebibyte_preflight_bound() {
    let Value(mut spec) =
        postcard::from_bytes::<Value<PlacementSpec>>(fixture!("placement-00.bin")).unwrap();
    spec.placement.voters = (0..60_000u64)
        .map(|key| (u64::MAX - key, u64::MAX))
        .collect();
    assert!(postcard::experimental::serialized_size(&Ref(&spec)).unwrap() > 1024 * 1024);
    assert_eq!(placement_digest(&spec), Err(DirectoryError::Capacity));
}

fn rejects_truncation<T: V1>(bytes: &[u8]) {
    for length in 0..bytes.len() {
        assert!(
            postcard::from_bytes::<Value<T>>(&bytes[..length]).is_err(),
            "accepted length {length}"
        );
    }
    let mut extended = bytes.to_vec();
    extended.push(0x5a);
    let (_, remainder) = postcard::take_from_bytes::<Value<T>>(&extended).unwrap();
    // The enclosing envelope must refuse this tail before state publication.
    assert_eq!(remainder, &[0x5a]);
}
#[test]
fn nested_truncation_and_hostile_container_hints_cannot_publish_partial_values() {
    rejects_truncation::<PlacementSpec>(fixture!("placement-17.bin"));
    rejects_truncation::<SessionFence>(fixture!("distinct-session-fence.bin"));
    rejects_truncation::<NodeEnrollment>(fixture!("distinct-node-enrollment.bin"));
    rejects_truncation::<ReplicaReady>(fixture!("distinct-replica-ready.bin"));
    rejects_truncation::<DelegationFence>(fixture!("distinct-delegation-fence.bin"));
    let enormous_count = postcard::to_allocvec(&u64::MAX).unwrap();
    assert!(postcard::from_bytes::<Value<Vec<PlacementSpec>>>(&enormous_count).is_err());
    assert!(postcard::from_bytes::<Value<Placement>>(&enormous_count).is_err());
    assert!(postcard::from_bytes::<Value<BTreeSet<RegionId>>>(&enormous_count).is_err());
}
