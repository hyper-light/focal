use super::super::record_codec::recovery::tests as ckpt;
use super::super::record_codec::{
    self, CodecError, EncodingLimits, EncodingPlan as RecordEncodingPlan, InspectionLimits,
    StructuralRecord, checkpoint, recovery, replay,
};
use super::super::report_tests as f;
use super::super::*;
use super::Affinity;
use focal_evidence::BuiltinNativeSchemas;
use focal_model::{ClaimStatus, ParticipantId, ValidationId, ValidationMode};

/// Four boundaries in affinity order: claim 1 (its rows and its cycles,
/// evaluations and results), the issuer (its outcomes and index buckets keyed
/// by participant), the first declaration (definitions) and the index
/// buckets (`0xb0`-prefixed affinities): five members, the first holding the
/// control affinity (meta, events, timers).
fn boundaries() -> [(Affinity, RangeId); 4] {
    let mut buckets = [0u8; 16];
    buckets[0] = 0xb0;
    [
        (ClaimId::from_u128(1).0, RangeId(2)),
        (ParticipantId::from_u128(61).0, RangeId(3)),
        (ValidationId::from_u128(101).0, RangeId(4)),
        (buckets, RangeId(5)),
    ]
}
fn split_all(core: &mut Core<NativeState>) {
    for (at, id) in boundaries() {
        core.split_native_range(at, id).unwrap();
    }
}
fn cancel(id: u128, expected: Binding) -> NativeInput {
    NativeInput {
        request: f::request(f::ISSUER, id),
        command: NativeCommand::Cancel { expected },
    }
}
/// The same workflow on any core: a claim with two requirements is posted
/// and both admissions begun, a second claim is created and cancelled.
fn drive(core: &mut Core<NativeState>) {
    f::publish(
        core,
        10,
        f::creation(
            1,
            1,
            &[
                (ValidationMode::Required, false),
                (ValidationMode::Observe, true),
            ],
            None,
        ),
    );
    f::publish(core, 20, f::post(2, f::binding(1)));
    for index in 1..=2 {
        let claim = core.native_claim(ClaimId::from_u128(1)).unwrap().binding();
        let state = core.native_evaluation(f::key(index)).unwrap().binding();
        f::publish(
            core,
            30,
            f::begin(10 + u128::from(index), claim, index, state),
        );
    }
    f::publish(core, 40, f::creation(50, 3, &[], None));
    let expected = core.native_claim(ClaimId::from_u128(3)).unwrap().binding();
    f::publish(core, 50, cancel(51, expected));
}
fn digest(core: &Core<NativeState>) -> ContentHash {
    checkpoint::rows_digest(
        core,
        EncodingLimits {
            bytes: 32 << 20,
            visits: 1_000_000_000,
            rows: 100_000,
        },
    )
    .unwrap()
}
fn checksum(bytes: &mut [u8]) {
    let at = bytes.len() - 32;
    let (payload, trailer) = bytes.split_at_mut(at);
    let mut hasher = blake3::Hasher::new_derive_key(checkpoint::HASH_DOMAIN);
    hasher.update(payload);
    trailer.copy_from_slice(hasher.finalize().as_bytes());
}
fn layout(core: &Core<NativeState>) -> Vec<(RangeId, Option<Affinity>)> {
    core.native_layout().boundaries().collect()
}
fn claims(core: &Core<NativeState>) -> Vec<ClaimId> {
    core.native_claims_from(None).collect()
}
fn evaluations(core: &Core<NativeState>) -> Vec<EvaluationKey> {
    core.native_evaluations_from(None, None).collect()
}

#[test]
fn one_versus_many_ranges_hold_identical_state_and_serve_identical_reads() {
    let mut single = f::core();
    let mut many = f::core();
    assert_eq!(layout(&many), vec![(many.state.rows.id(), None)]);
    split_all(&mut many);
    let expected_layout: Vec<_> = std::iter::once((many.state.rows.id(), None))
        .chain(boundaries().into_iter().map(|(at, id)| (id, Some(at))))
        .collect();
    assert_eq!(layout(&many), expected_layout);
    drive(&mut single);
    drive(&mut many);
    assert_eq!(digest(&many), digest(&single));
    assert_eq!(many.native_sequence(), single.native_sequence());
    assert_eq!(many.native_stats().entries, single.native_stats().entries);
    assert_eq!(layout(&many), expected_layout);
    // Every member holds rows: the write sets were divided among them and
    // published as one; every member sits at the group's prefix.
    assert!(many.state.rows.stores.iter().all(|store| !store.is_empty()));
    assert!(
        many.state
            .rows
            .stores
            .iter()
            .all(|store| store.prefix() == many.native_sequence().0)
    );
    assert_eq!(claims(&many), claims(&single));
    assert_eq!(evaluations(&many), evaluations(&single));
    assert_eq!(
        many.native_claim(ClaimId::from_u128(1)).unwrap().status(),
        ClaimStatus::Posted
    );
    assert_eq!(
        many.native_claim(ClaimId::from_u128(3)).unwrap().status(),
        single.native_claim(ClaimId::from_u128(3)).unwrap().status()
    );
    assert_eq!(
        many.native_evaluation(f::key(2)).unwrap().binding(),
        single.native_evaluation(f::key(2)).unwrap().binding()
    );
    assert_eq!(
        many.native_outcome(f::request(f::ISSUER, 2)),
        single.native_outcome(f::request(f::ISSUER, 2))
    );
    for sequence in 1..=many.native_sequence().0 {
        assert_eq!(
            format!("{:?}", many.native_event(SessionSeq(sequence), 0)),
            format!("{:?}", single.native_event(SessionSeq(sequence), 0))
        );
    }
    // A read pins every member at one prefix and projects across them.
    let read = many.pin_native(0, 100).unwrap();
    assert_eq!(read.sequence(), many.native_sequence());
    assert_eq!(
        read.with_claim(ClaimId::from_u128(1), 0, |claim| claim.status())
            .unwrap(),
        Some(ClaimStatus::Posted)
    );
    assert_eq!(
        read.recorded(f::request(f::ISSUER, 2), 0).unwrap(),
        single.native_outcome(f::request(f::ISSUER, 2))
    );
    assert_eq!(
        read.with_definition(f::key(1).validation, 0, |definition| definition.binding())
            .unwrap(),
        Some(
            single
                .native_definition(f::key(1).validation)
                .unwrap()
                .binding()
        )
    );
    assert_eq!(many.native_stats().pinned_snapshots, 1);
    assert_eq!(many.native_layout().epoch(), 4);
    // A layout change under a lease expires the lease with the members it
    // pinned; releasing it afterwards is complete and holds nothing.
    many.split_native_range(ClaimId::from_u128(3).0, RangeId(9))
        .unwrap();
    assert_eq!(many.native_layout().epoch(), 5);
    assert!(matches!(
        read.with_claim(ClaimId::from_u128(1), 0, |claim| claim.status()),
        Err(MemoryError::LeaseExpired)
    ));
    many.release_native(&read).unwrap();
    assert_eq!(many.native_stats().pinned_snapshots, 0);
    many.merge_native_range(1).unwrap();
    assert_eq!(many.native_layout().epoch(), 6);
    assert_eq!(many.native_layout().len(), 5);
    assert_eq!(digest(&many), digest(&single));
    // A read taken after the change pins the new members.
    let read = many.pin_native(1, 100).unwrap();
    assert_eq!(
        read.with_claim(ClaimId::from_u128(1), 1, |claim| claim.status())
            .unwrap(),
        Some(ClaimStatus::Posted)
    );
    many.release_native(&read).unwrap();
    assert_eq!(many.native_stats().pinned_snapshots, 0);
    // An existing boundary, a known identity and a full layout are refused.
    assert!(matches!(
        many.split_native_range(ClaimId::from_u128(1).0, RangeId(9)),
        Err(NativeError::Contract(ContractError::InvalidManifest))
    ));
    assert!(matches!(
        many.split_native_range(ClaimId::from_u128(3).0, RangeId(3)),
        Err(NativeError::Contract(ContractError::InvalidManifest))
    ));
    many.limits.max_ranges = 5;
    assert!(matches!(
        many.split_native_range(ClaimId::from_u128(3).0, RangeId(9)),
        Err(NativeError::Capacity("range layout members"))
    ));
    many.limits.max_ranges = 64;
    // A further split inside a member and a merge keep the rows.
    many.split_native_range(ClaimId::from_u128(3).0, RangeId(9))
        .unwrap();
    assert_eq!(many.native_layout().len(), 6);
    assert_eq!(digest(&many), digest(&single));
    many.merge_native_range(1).unwrap();
    assert_eq!(many.native_layout().len(), 5);
    assert_eq!(
        layout(&many).get(1).copied(),
        Some((RangeId(2), Some(ClaimId::from_u128(1).0)))
    );
    assert_eq!(digest(&many), digest(&single));
    assert!(matches!(
        many.merge_native_range(4),
        Err(NativeError::Contract(ContractError::InvalidManifest))
    ));
    // Writes continue across the changed layout.
    f::publish(&mut many, 60, f::creation(70, 4, &[], None));
    f::publish(&mut single, 60, f::creation(70, 4, &[], None));
    assert_eq!(digest(&many), digest(&single));
    assert_eq!(claims(&many), claims(&single));
    while many.native_layout().len() > 1 {
        many.merge_native_range(0).unwrap();
    }
    assert_eq!(layout(&many), vec![(many.state.rows.id(), None)]);
    assert_eq!(digest(&many), digest(&single));
}

#[test]
fn checkpoints_carry_the_layout_and_restore_it_with_durable_member_identities() {
    let path = tempfile::tempdir().unwrap();
    let store = ckpt::store(path.path());
    let mut single = f::core();
    let mut many = f::core();
    split_all(&mut many);
    drive(&mut single);
    drive(&mut many);
    let bytes = ckpt::encode(&many);
    let structural = ckpt::inspect(&bytes);
    assert_eq!(structural.members(), 5);
    assert_eq!(structural.header().range, many.state.rows.id());
    let budget = ckpt::budget();
    let restored = recovery::restore(
        &structural,
        RangeId(777),
        ckpt::limits(many.limits),
        budget.clone(),
        &store,
        &BuiltinNativeSchemas,
    )
    .unwrap();
    // Members keep the identities the checkpoint names; the producer is
    // the caller's fresh one.
    assert_eq!(layout(&restored), layout(&many));
    assert_eq!(
        restored.native_layout().epoch(),
        many.native_layout().epoch()
    );
    assert_eq!(restored.native_layout().epoch(), 4);
    assert_eq!(restored.state.rows.id(), RangeId(777));
    assert_eq!(digest(&restored), digest(&single));
    ckpt::compare(&many, &restored);
    assert_eq!(claims(&restored), claims(&single));
    assert_eq!(evaluations(&restored), evaluations(&single));
    // The restored group keeps working and can be checkpointed again.
    let mut restored = restored;
    f::publish(&mut restored, 60, f::creation(70, 4, &[], None));
    f::publish(&mut single, 60, f::creation(70, 4, &[], None));
    assert_eq!(digest(&restored), digest(&single));
    let again = ckpt::encode(&restored);
    assert_eq!(ckpt::inspect(&again).members(), 5);
    drop(restored);
    assert_eq!(budget.stats().used, 0);
    // A one-member checkpoint restores its member identity too.
    let bytes = ckpt::encode(&single);
    let restored = recovery::restore(
        &ckpt::inspect(&bytes),
        RangeId(778),
        ckpt::limits(single.limits),
        ckpt::budget(),
        &store,
        &BuiltinNativeSchemas,
    )
    .unwrap();
    assert_eq!(layout(&restored), vec![(single.state.rows.id(), None)]);
    assert_eq!(restored.state.rows.id(), RangeId(778));
    // The owner's member bound applies at restore.
    let mut narrow = ckpt::limits(many.limits);
    narrow.native.max_ranges = 2;
    assert!(matches!(
        recovery::restore(
            &ckpt::inspect(&ckpt::encode(&many)),
            RangeId(779),
            narrow,
            ckpt::budget(),
            &store,
            &BuiltinNativeSchemas,
        ),
        Err(NativeError::Capacity("range layout members"))
    ));
}

#[test]
fn records_replay_into_a_differently_laid_out_incarnation() {
    let path = tempfile::tempdir().unwrap();
    let store = ckpt::store(path.path());
    let mut single = f::core();
    let mut many = f::core();
    split_all(&mut many);
    let inputs = [
        (
            10,
            f::creation(
                1,
                1,
                &[
                    (ValidationMode::Required, false),
                    (ValidationMode::Observe, true),
                ],
                None,
            ),
        ),
        (20, f::post(2, f::binding(1))),
        (40, f::creation(50, 3, &[], None)),
    ];
    for (time, input) in inputs {
        let prepared = f::prepared(single.prepare_native(
            f::context(input.request.principal, time),
            input,
            &[],
        ));
        let plan = RecordEncodingPlan::prepare(
            &prepared,
            EncodingLimits {
                bytes: 32 << 20,
                visits: 1_000_000_000,
                rows: 100_000,
            },
        )
        .unwrap();
        let mut bytes = vec![0; plan.quote().bytes];
        plan.write_into(&mut bytes).unwrap();
        let record = StructuralRecord::inspect(
            &bytes,
            InspectionLimits {
                bytes: bytes.len(),
                visits: 1_000_000_000,
                rows: 100_000,
                row_bytes: 32 << 20,
            },
        )
        .unwrap();
        let replayed = replay::prepare(
            &many,
            &record,
            single.state.rows.id(),
            ckpt::limits(many.limits),
            &store,
            &BuiltinNativeSchemas,
        )
        .unwrap();
        assert_eq!(replayed.outcome(), prepared.outcome());
        assert_eq!(replayed.mutation_count(), prepared.mutation_count());
        single.publish_native(prepared).unwrap();
        many.publish_native(replayed).unwrap();
        assert_eq!(digest(&many), digest(&single));
    }
    assert_eq!(many.native_layout().len(), 5);
    assert_eq!(claims(&many), claims(&single));
    let _ = record_codec::VERSION;
}

#[test]
fn layout_frames_refuse_disorder_duplicates_and_a_bounded_first_member() {
    let mut many = f::core();
    split_all(&mut many);
    drive(&mut many);
    let bytes = ckpt::encode(&many);
    // The header: magic (8), version (2), profile (1), ledger (32), range
    // (16), prefix (8), row count (8), then the layout: a member count (4),
    // the epoch (8), the first member (16 + 1) and four bounded members
    // (16 + 1 + 16).
    const LAYOUT_AT: usize = 75;
    const FIRST_AT: usize = LAYOUT_AT + 12;
    const SECOND_AT: usize = FIRST_AT + 17;
    const THIRD_AT: usize = SECOND_AT + 33;
    assert_eq!(&bytes[LAYOUT_AT..LAYOUT_AT + 4], &5u32.to_le_bytes());
    assert_eq!(&bytes[LAYOUT_AT + 4..FIRST_AT], &4u64.to_le_bytes());
    assert_eq!(bytes[FIRST_AT + 16], 0);
    assert_eq!(bytes[SECOND_AT + 16], 1);
    let inspect = |bytes: &[u8]| {
        checkpoint::StructuralCheckpoint::inspect(
            bytes,
            InspectionLimits {
                bytes: bytes.len(),
                visits: 1_000_000_000,
                rows: 100_000,
                row_bytes: 32 << 20,
            },
        )
        .map(|_| ())
    };
    inspect(&bytes).unwrap();
    // Starts out of order.
    let mut swapped = bytes.clone();
    let second: [u8; 16] = swapped[SECOND_AT + 17..SECOND_AT + 33].try_into().unwrap();
    let third: [u8; 16] = swapped[THIRD_AT + 17..THIRD_AT + 33].try_into().unwrap();
    swapped[SECOND_AT + 17..SECOND_AT + 33].copy_from_slice(&third);
    swapped[THIRD_AT + 17..THIRD_AT + 33].copy_from_slice(&second);
    checksum(&mut swapped);
    assert_eq!(
        inspect(&swapped),
        Err(CodecError::InvalidTag("checkpoint layout order"))
    );
    // A repeated identity.
    let mut repeated = bytes.clone();
    let first_id: [u8; 16] = repeated[FIRST_AT..FIRST_AT + 16].try_into().unwrap();
    repeated[THIRD_AT..THIRD_AT + 16].copy_from_slice(&first_id);
    checksum(&mut repeated);
    assert_eq!(
        inspect(&repeated),
        Err(CodecError::InvalidTag("checkpoint layout identity"))
    );
    // A bounded first member (its flag says it has a start it lacks).
    let mut bounded = bytes.clone();
    bounded[FIRST_AT + 16] = 1;
    checksum(&mut bounded);
    assert!(inspect(&bounded).is_err());
    // No members at all.
    let mut none = bytes.clone();
    none[LAYOUT_AT..LAYOUT_AT + 4].copy_from_slice(&0u32.to_le_bytes());
    checksum(&mut none);
    assert_eq!(
        inspect(&none),
        Err(CodecError::InvalidTag("checkpoint layout members"))
    );
    // The checksum guards every byte of the layout: a changed identity that
    // still names a distinct member is caught by the trailer.
    let mut unsigned = bytes;
    unsigned[THIRD_AT + 33 + 2] ^= 0x01;
    assert_eq!(
        inspect(&unsigned),
        Err(CodecError::InvalidTag("checkpoint checksum"))
    );
}
