use super::*;
use focal_core::native::NativeLimits;
use focal_model::{SessionId, TenantId};

fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(11),
        session: SessionId::from_u128(12),
    }
}
fn core(authored: bool, range: u128) -> Core<NativeState> {
    let budget = MemoryBudget::new(32 << 20, 8 << 20).unwrap();
    if authored {
        Core::new_native_authored(ledger(), RangeId(range), NativeLimits::default(), budget)
            .unwrap()
    } else {
        Core::new_native(ledger(), RangeId(range), NativeLimits::default(), budget).unwrap()
    }
}
fn configuration() -> MembershipConfiguration {
    MembershipConfiguration {
        voters: vec![1, 3],
        learners: vec![4],
        voters_outgoing: vec![1, 2],
        learners_next: vec![2],
        auto_leave: true,
    }
}
fn metadata(profile: NativeContentProfile) -> Metadata {
    let decoder = format_hash();
    let cluster = [21; 16];
    let group = [22; 16];
    Metadata {
        cluster,
        group,
        applied_raft: 9,
        applied_term: 4,
        configuration_index: 8,
        recording_range: None,
        recording_term: 0,
        records_floor: 0,
        activation_index: 3,
        ancillary: AncillaryProfile::NativeOnlyV1,
        activation: Activation {
            decoder,
            durable_floor: decoder,
            genesis: genesis(cluster, group, ledger(), profile, decoder),
        },
    }
}
fn encoded(authored: bool) -> (Vec<u8>, Header, Quote) {
    let core = core(authored, 31);
    let configuration = configuration();
    let profile = if authored {
        NativeContentProfile::AuthoredV1
    } else {
        NativeContentProfile::ProjectionOnly
    };
    let plan =
        EncodingPlan::prepare(&core, metadata(profile), &configuration, Limits::default()).unwrap();
    let mut bytes = vec![0; plan.quote().bytes];
    plan.write_into(&mut bytes).unwrap();
    (bytes, plan.header().unwrap(), plan.quote())
}
fn checksum(bytes: &mut [u8]) {
    let at = bytes.len() - 32;
    let mut hash = blake3::Hasher::new_derive_key(HASH_DOMAIN);
    hash.update(&bytes[..at]);
    bytes[at..].copy_from_slice(hash.finalize().as_bytes());
}

#[test]
fn both_actual_core_profiles_roundtrip_exact_physical_raft_membership_and_genesis_fields() {
    for authored in [false, true] {
        let (bytes, header, quote) = encoded(authored);
        let checkpoint = Checkpoint::inspect(&bytes, Limits::default()).unwrap();
        assert_eq!(checkpoint.header(), header);
        assert_eq!(header.hash, quote.hash);
        assert_eq!(header.range, RangeId(31));
        assert_eq!(header.metadata.applied_raft, 9);
        assert_eq!(header.metadata.applied_term, 4);
        assert_eq!(header.metadata.configuration_index, 8);
        assert_eq!(checkpoint.core().header().hash, header.core_hash);
        assert_eq!(checkpoint.core_bytes().len() as u64, header.core_bytes);
        checkpoint.configuration_matches(&configuration()).unwrap();
        let mut wrong = configuration();
        wrong.voters[1] = 5;
        assert!(checkpoint.configuration_matches(&wrong).is_err());
        // A new runtime range does not rewrite the stable physical genesis.
        assert_eq!(
            genesis(
                header.metadata.cluster,
                header.metadata.group,
                header.ledger,
                header.profile,
                header.metadata.activation.decoder
            ),
            header.metadata.activation.genesis
        );
    }
}

#[test]
fn frame_refuses_repaired_identity_floor_activation_disabled_legacy_and_nested_substitution() {
    let (bytes, _, _) = encoded(false);
    // Fixed framing for a genesis native prefix with no recording range.
    for (offset, value) in [
        (8, 4),    // Unknown enclosing schema.
        (10, 99),  // Cluster disagrees with retained stable genesis.
        (74, 1),   // Native content profile disagrees with root/genesis.
        (75, 99),  // Core range substitution.
        (99, 0),   // Applied index below native/membership history.
        (107, 0),  // Nonzero applied index with missing applied term.
        (115, 10), // Configuration from beyond this checkpoint.
        (123, 2),  // Noncanonical optional recording range.
        (124, 1),  // Recording term without any native record.
        (132, 2),  // Records floor beyond an imported prefix.
        (140, 0),  // Activation index cannot be zero.
        (140, 10), // Activation index beyond the applied index.
        (148, 1),  // Legacy successor activation is unsupported.
        (181, 99), // Durable decoder floor differs from compiled decoder.
        (245, 1),  // Unknown ancillary profile.
        (246, 1),  // Cursor state cannot be silently discarded.
        (247, 1),  // Cursor receipt/owner state.
        (248, 1),  // Retained delta stream.
        (249, 1),  // Managed request streams.
        (250, 1),  // Placement state.
    ] {
        let mut corrupt = bytes.clone();
        corrupt[offset] = value;
        checksum(&mut corrupt);
        assert!(
            Checkpoint::inspect(&corrupt, Limits::default()).is_err(),
            "offset {offset}"
        );
    }
    let checkpoint = Checkpoint::inspect(&bytes, Limits::default()).unwrap();
    let core_offset = checkpoint.core_bytes().as_ptr() as usize - bytes.as_ptr() as usize;
    let mut corrupt = bytes.clone();
    corrupt[core_offset + 43] ^= 1; // Actual nested range; repair both checksums.
    let root_end = core_offset + checkpoint.core_bytes().len() - 32;
    let mut hash = blake3::Hasher::new_derive_key("focal.native.checkpoint.v2");
    hash.update(&corrupt[core_offset..root_end]);
    corrupt[root_end..root_end + 32].copy_from_slice(hash.finalize().as_bytes());
    checksum(&mut corrupt);
    assert!(Checkpoint::inspect(&corrupt, Limits::default()).is_err());
    for length in [0, 8, 40, bytes.len() - 1] {
        assert!(Checkpoint::inspect(&bytes[..length], Limits::default()).is_err());
    }
    let mut trailing = bytes.clone();
    trailing.insert(bytes.len() - 32, 0);
    checksum(&mut trailing);
    assert!(Checkpoint::inspect(&trailing, Limits::default()).is_err());
    assert!(Checkpoint::inspect(b"FOCALSS5populated-legacy-state", Limits::default()).is_err());
}

#[test]
fn exact_work_and_output_limits_are_cumulative_and_wrong_sized_destination_is_unchanged() {
    let core = core(false, 31);
    let configuration = configuration();
    let meta = metadata(NativeContentProfile::ProjectionOnly);
    let plan = EncodingPlan::prepare(&core, meta, &configuration, Limits::default()).unwrap();
    let quote = plan.quote();
    let precise = Limits {
        bytes: quote.bytes,
        visits: quote.preparation_visits,
        ..Limits::default()
    };
    assert_eq!(
        EncodingPlan::prepare(&core, meta, &configuration, precise)
            .unwrap()
            .quote(),
        quote
    );
    assert!(
        EncodingPlan::prepare(
            &core,
            meta,
            &configuration,
            Limits {
                visits: precise.visits - 1,
                ..precise
            }
        )
        .is_err()
    );
    assert!(
        EncodingPlan::prepare(
            &core,
            meta,
            &configuration,
            Limits {
                bytes: precise.bytes - 1,
                ..precise
            }
        )
        .is_err()
    );
    let mut short = vec![0xAA; quote.bytes - 1];
    assert!(plan.write_into(&mut short).is_err());
    assert!(short.iter().all(|byte| *byte == 0xAA));
    let mut bytes = vec![0; quote.bytes];
    plan.write_into(&mut bytes).unwrap();
    let parsed = Checkpoint::inspect(&bytes, Limits::default()).unwrap();
    let parse_work = parsed.visits_used();
    parsed.configuration_matches(&configuration).unwrap();
    let total = parsed.visits_used();
    let parsed = Checkpoint::inspect(
        &bytes,
        Limits {
            visits: total,
            ..Limits::default()
        },
    )
    .unwrap();
    parsed.configuration_matches(&configuration).unwrap();
    assert_eq!(parsed.remaining_visits(), 0);
    assert!(parsed.configuration_matches(&configuration).is_err());
    assert!(
        Checkpoint::inspect(
            &bytes,
            Limits {
                visits: parse_work - 1,
                ..Limits::default()
            }
        )
        .is_err()
    );
    let parsed = Checkpoint::inspect(
        &bytes,
        Limits {
            visits: total - 1,
            ..Limits::default()
        },
    )
    .unwrap();
    assert!(parsed.configuration_matches(&configuration).is_err());
}

#[test]
fn output_is_precharged_refunded_and_stream_errors_keep_the_original_value() {
    let core = core(false, 31);
    let configuration = configuration();
    let plan = EncodingPlan::prepare(
        &core,
        metadata(NativeContentProfile::ProjectionOnly),
        &configuration,
        Limits::default(),
    )
    .unwrap();
    let bytes = plan.quote().output_charge;
    let budget = MemoryBudget::new(bytes, bytes).unwrap();
    let encoded = plan.encode_in(&budget).unwrap();
    assert_eq!(encoded.charge(), bytes);
    assert_eq!(budget.stats().used, bytes);
    let (buffer, permit) = encoded.into_parts();
    assert_eq!(budget.stats().used, bytes);
    drop(buffer);
    drop(permit);
    assert_eq!(budget.stats().used, 0);
    let tight = MemoryBudget::new(bytes - 1, bytes - 1).unwrap();
    assert!(plan.encode_in(&tight).is_err());
    assert_eq!(tight.stats().used, 0);
    #[derive(Debug, PartialEq, Eq)]
    struct OriginalError(u64);
    let mut calls = 0;
    let failure = plan.write_with(|_| {
        calls += 1;
        Err(OriginalError(87))
    });
    assert!(matches!(
        failure,
        Err(WriteError::Output(OriginalError(87)))
    ));
    assert_eq!(calls, 1);
}

#[test]
fn canonical_joint_membership_validation_rejects_overlap_duplicates_and_unfunded_counts() {
    let core = core(false, 31);
    let meta = metadata(NativeContentProfile::ProjectionOnly);
    let mut bad = configuration();
    bad.learners.push(1);
    assert!(EncodingPlan::prepare(&core, meta, &bad, Limits::default()).is_err());
    let mut bad = configuration();
    bad.voters.push(3);
    assert!(EncodingPlan::prepare(&core, meta, &bad, Limits::default()).is_err());
    let mut bad = configuration();
    bad.learners_next = vec![4];
    assert!(EncodingPlan::prepare(&core, meta, &bad, Limits::default()).is_err());
    let mut bad = configuration();
    bad.voters_outgoing.clear();
    assert!(EncodingPlan::prepare(&core, meta, &bad, Limits::default()).is_err());
    let (bytes, _, _) = encoded(false);
    assert!(
        Checkpoint::inspect(
            &bytes,
            Limits {
                members: 5,
                ..Limits::default()
            }
        )
        .is_err()
    );
    let mut bad = bytes;
    bad[251..255].copy_from_slice(&u32::MAX.to_le_bytes());
    checksum(&mut bad);
    assert!(Checkpoint::inspect(&bad, Limits::default()).is_err());
}
