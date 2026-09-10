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
        (8, 5),    // Unknown enclosing schema.
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
    // The form byte precedes the Core byte count (8) and hash (32); an
    // unknown form and a seeded form claiming an inline body are refused.
    for form in [1u8, 2] {
        let mut corrupt = bytes.clone();
        corrupt[core_offset - 41] = form;
        checksum(&mut corrupt);
        assert!(
            Checkpoint::inspect(&corrupt, Limits::default()).is_err(),
            "form {form}"
        );
    }
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

fn seed_store(directory: &std::path::Path) -> focal_evidence::SeedStore {
    focal_evidence::SeedStore::open(
        directory.join("seeds"),
        focal_memory::DiskBudget::new(focal_memory::DiskBudgetConfig::default()).unwrap(),
    )
    .unwrap()
}
fn budget() -> MemoryBudget {
    MemoryBudget::new(64 << 20, 8 << 20).unwrap()
}

#[test]
fn a_root_beyond_the_inline_bound_is_seeded_and_assembled_back_exactly() {
    let directory = tempfile::tempdir().unwrap();
    let mut seeds = seed_store(directory.path());
    let core = core(false, 31);
    let configuration = configuration();
    let inline = EncodingPlan::prepare(
        &core,
        metadata(NativeContentProfile::ProjectionOnly),
        &configuration,
        Limits::default(),
    )
    .unwrap();
    assert!(!inline.seeded());
    let inline_bytes = inline.encode_in(&budget()).unwrap();
    let limits = Limits {
        inline_bytes: 16,
        ..Limits::default()
    };
    let plan = EncodingPlan::prepare(
        &core,
        metadata(NativeContentProfile::ProjectionOnly),
        &configuration,
        limits,
    )
    .unwrap();
    assert!(plan.seeded());
    // The inline paths refuse a seeded plan; only the seed store can encode it.
    let mut scratch = vec![0; plan.quote().bytes];
    assert!(matches!(plan.write_into(&mut scratch), Err(Error::Seeded)));
    assert!(matches!(plan.encode_in(&budget()), Err(Error::Seeded)));
    assert!(matches!(
        plan.write_with(|_| Ok::<(), ()>(())),
        Err(WriteError::Codec(Error::Seeded))
    ));
    let encoded = plan.encode_in_seeded(&budget(), &mut seeds).unwrap();
    assert_eq!(encoded.bytes().len(), plan.quote().bytes);
    assert!(encoded.bytes().len() < inline_bytes.bytes().len());
    // The seeded frame names one chunk holding the whole root.
    let manifest = Checkpoint::describe(encoded.bytes(), limits)
        .unwrap()
        .expect("seeded form");
    assert_eq!(manifest.len(), 1);
    let chunk = manifest.chunks().next().unwrap().unwrap();
    assert_eq!(u64::from(chunk.length), manifest.header().core_bytes);
    assert!(seeds.contains(chunk.hash));
    assert!(matches!(
        Checkpoint::inspect(encoded.bytes(), limits),
        Err(Error::Seeded)
    ));
    // Assembled from the seeds, the checkpoint is the inline one's equal.
    assert!(manifest.missing(&seeds.reader()).unwrap().is_empty());
    let assembled = manifest.assemble(&seeds.reader(), &budget()).unwrap();
    let seeded = Checkpoint::inspect_seeded(encoded.bytes(), assembled.bytes(), limits).unwrap();
    let plain = Checkpoint::inspect(inline_bytes.bytes(), Limits::default()).unwrap();
    assert_eq!(seeded.core_bytes(), plain.core_bytes());
    assert_eq!(seeded.header().core_hash, plain.header().core_hash);
    assert_eq!(seeded.header().metadata, plain.header().metadata);
    seeded.configuration_matches(&configuration).unwrap();
    // A wrong-length assembly, an absent chunk and a corrupt one are refused.
    assert!(matches!(
        Checkpoint::inspect_seeded(encoded.bytes(), &assembled.bytes()[1..], limits),
        Err(Error::Invalid("assembled core length"))
    ));
    let empty = tempfile::tempdir().unwrap();
    let elsewhere = seed_store(empty.path());
    assert_eq!(
        manifest.missing(&elsewhere.reader()).unwrap(),
        vec![chunk.hash]
    );
    assert!(matches!(
        manifest.assemble(&elsewhere.reader(), &budget()),
        Err(SeedError::Missing(hash)) if hash == chunk.hash
    ));
    std::fs::write(
        directory
            .path()
            .join("seeds")
            .join(format!("{}.seed", chunk.hash)),
        b"not the chunk",
    )
    .unwrap();
    assert!(matches!(
        manifest.assemble(&seeds.reader(), &budget()),
        Err(SeedError::Seeds(focal_evidence::ContentError::Corrupt))
    ));
    // A frame whose table disagrees with its root length is refused whole.
    let mut forged = encoded.bytes().to_vec();
    let table_end = forged.len() - 32;
    forged[table_end - 4..table_end].copy_from_slice(&7u32.to_le_bytes());
    checksum(&mut forged);
    assert!(matches!(
        Checkpoint::describe(&forged, limits).map(|_| ()),
        Err(Error::Invalid("seed chunk table")) | Ok(())
    ));
}

#[test]
fn a_movement_section_rides_both_forms_and_every_forgery_of_it_is_refused() {
    let directory = tempfile::tempdir().unwrap();
    let mut seeds = seed_store(directory.path());
    let core = core(false, 31);
    let configuration = configuration();
    let section = [0x5a; 300];
    // Inline: the section is carried after the configuration and read back.
    let plan = EncodingPlan::prepare_with(
        &core,
        metadata(NativeContentProfile::ProjectionOnly),
        &configuration,
        Some(&section),
        Limits::default(),
    )
    .unwrap();
    let plain = EncodingPlan::prepare(
        &core,
        metadata(NativeContentProfile::ProjectionOnly),
        &configuration,
        Limits::default(),
    )
    .unwrap();
    assert_eq!(plan.quote().bytes, plain.quote().bytes + 4 + section.len());
    let bytes = plan.encode_in(&budget()).unwrap();
    let checkpoint = Checkpoint::inspect(bytes.bytes(), Limits::default()).unwrap();
    assert_eq!(checkpoint.movement(), &section[..]);
    assert_eq!(
        checkpoint.header().metadata,
        plan.header().unwrap().metadata
    );
    let plain_bytes = plain.encode_in(&budget()).unwrap();
    let none = Checkpoint::inspect(plain_bytes.bytes(), Limits::default()).unwrap();
    assert!(none.movement().is_empty());
    // Seeded: the manifest carries it too.
    let limits = Limits {
        inline_bytes: 16,
        ..Limits::default()
    };
    let seeded = EncodingPlan::prepare_with(
        &core,
        metadata(NativeContentProfile::ProjectionOnly),
        &configuration,
        Some(&section),
        limits,
    )
    .unwrap();
    let encoded = seeded.encode_in_seeded(&budget(), &mut seeds).unwrap();
    let manifest = Checkpoint::describe(encoded.bytes(), limits)
        .unwrap()
        .expect("seeded form");
    assert_eq!(manifest.movement(), &section[..]);
    let assembled = manifest.assemble(&seeds.reader(), &budget()).unwrap();
    let restored = Checkpoint::inspect_seeded(encoded.bytes(), assembled.bytes(), limits).unwrap();
    assert_eq!(restored.movement(), &section[..]);
    // An empty or oversized section is refused at the plan, an unknown
    // ancillary byte, a zero length and a length past the frame at the reader.
    assert!(matches!(
        EncodingPlan::prepare_with(
            &core,
            metadata(NativeContentProfile::ProjectionOnly),
            &configuration,
            Some(&[]),
            Limits::default()
        ),
        Err(Error::Invalid("movement section"))
    ));
    let too_big = vec![1u8; Limits::default().movement_bytes + 1];
    assert!(matches!(
        EncodingPlan::prepare_with(
            &core,
            metadata(NativeContentProfile::ProjectionOnly),
            &configuration,
            Some(&too_big),
            Limits::default()
        ),
        Err(Error::Invalid("movement section"))
    ));
    let flag = 245;
    let length_at = {
        // The section length follows the configuration: locate it by the
        // known layout of this fixture's bytes.
        let section_start = bytes
            .bytes()
            .windows(section.len())
            .position(|window| window == section)
            .unwrap();
        section_start - 4
    };
    for (offset, value) in [
        (flag, 2u8),
        (flag, 0),
        (length_at, 0),
        (length_at + 3, 0x7f),
    ] {
        let mut corrupt = bytes.bytes().to_vec();
        corrupt[offset] = value;
        checksum(&mut corrupt);
        assert!(
            Checkpoint::inspect(&corrupt, Limits::default()).is_err(),
            "offset {offset} value {value}"
        );
    }
}

/// The retention section (26 §3) rides both forms beside the movement
/// section and is absent from a frame that carries none.
#[test]
fn a_retention_section_rides_both_forms_beside_the_movement_section() {
    let directory = tempfile::tempdir().unwrap();
    let mut seeds = seed_store(directory.path());
    let core = core(false, 31);
    let configuration = configuration();
    let section = [0x5a; 40];
    let retention = RetentionSection {
        archived_through: SessionSeq(9),
        retired_families: 4,
    };
    let plan = EncodingPlan::prepare_with_sections(
        &core,
        metadata(NativeContentProfile::ProjectionOnly),
        &configuration,
        Some(&section),
        Some(retention),
        Limits::default(),
    )
    .unwrap();
    let with_movement = EncodingPlan::prepare_with(
        &core,
        metadata(NativeContentProfile::ProjectionOnly),
        &configuration,
        Some(&section),
        Limits::default(),
    )
    .unwrap();
    assert_eq!(plan.quote().bytes, with_movement.quote().bytes + 16);
    let bytes = plan.encode_in(&budget()).unwrap();
    let checkpoint = Checkpoint::inspect(bytes.bytes(), Limits::default()).unwrap();
    assert_eq!(checkpoint.retention(), Some(retention));
    assert_eq!(checkpoint.movement(), &section[..]);
    let none = with_movement.encode_in(&budget()).unwrap();
    assert_eq!(
        Checkpoint::inspect(none.bytes(), Limits::default())
            .unwrap()
            .retention(),
        None
    );
    // Retention without movement, inline and seeded.
    let alone = EncodingPlan::prepare_with_sections(
        &core,
        metadata(NativeContentProfile::ProjectionOnly),
        &configuration,
        None,
        Some(retention),
        Limits::default(),
    )
    .unwrap();
    let alone_bytes = alone.encode_in(&budget()).unwrap();
    let inspected = Checkpoint::inspect(alone_bytes.bytes(), Limits::default()).unwrap();
    assert_eq!(inspected.retention(), Some(retention));
    assert!(inspected.movement().is_empty());
    let limits = Limits {
        inline_bytes: 16,
        ..Limits::default()
    };
    let seeded = EncodingPlan::prepare_with_sections(
        &core,
        metadata(NativeContentProfile::ProjectionOnly),
        &configuration,
        None,
        Some(retention),
        limits,
    )
    .unwrap();
    let encoded = seeded.encode_in_seeded(&budget(), &mut seeds).unwrap();
    let manifest = Checkpoint::describe(encoded.bytes(), limits)
        .unwrap()
        .expect("seeded form");
    assert_eq!(manifest.retention(), Some(retention));
    let assembled = manifest.assemble(&seeds.reader(), &budget()).unwrap();
    let restored = Checkpoint::inspect_seeded(encoded.bytes(), assembled.bytes(), limits).unwrap();
    assert_eq!(restored.retention(), Some(retention));
    // Every flip of the section or its flag byte is refused by the digest.
    let raw = alone_bytes.bytes();
    let flag = raw
        .windows(6)
        .position(|window| window == [0, 1, 0, 0, 0, 0])
        .expect("the ancillary bytes");
    for offset in [flag + 1, raw.len() - 33 - 8] {
        let mut forged = raw.to_vec();
        forged[offset] ^= 1;
        assert!(
            Checkpoint::inspect(&forged, Limits::default()).is_err(),
            "offset {offset}"
        );
    }
}
