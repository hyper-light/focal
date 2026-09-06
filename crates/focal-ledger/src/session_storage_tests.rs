// Fixed original Session bytes; fixture generation is intentionally absent from
// the executable test tree. See fixtures/durable-session-v1/README.md.
use focal_model::durable_v1::V1;

fn fixture(name: &str) -> Vec<u8> {
    std::fs::read(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("fixtures/durable-session-v1")
            .join(name),
    )
    .unwrap()
}
fn fixed<T: V1>(name: &str) -> T {
    let bytes = fixture(name);
    let (value, tail) = durable_session_v1::take(&bytes).unwrap();
    assert!(tail.is_empty(), "{name}");
    value
}
fn verify_rows<T: V1>(name: &str, count: usize) {
    let rows: Vec<T> = fixed(name);
    assert_eq!(rows.len(), count, "{name}");
    let bytes = fixture(name);
    assert_eq!(
        durable_session_v1::encode(&[], &rows, bytes.len()).unwrap(),
        bytes,
        "{name}"
    );
    assert!(matches!(
        durable_session_v1::encode(&[], &rows, bytes.len() - 1),
        Err(LedgerError::Capacity)
    ));
}
fn verify_envelopes<T: V1>(stem: &str, magic: &[u8], count: usize) {
    for index in 0..count {
        let name = format!("{stem}-{index:02}.bin");
        let bytes = fixture(&name);
        let (value, tail): (T, _) =
            durable_session_v1::take(bytes.strip_prefix(magic).unwrap()).unwrap();
        assert!(tail.is_empty(), "{name}");
        assert_eq!(
            durable_session_v1::encode(magic, &value, bytes.len()).unwrap(),
            bytes,
            "{name}"
        );
        let mut suffixed = bytes[magic.len()..].to_vec();
        suffixed.extend_from_slice(&[0xff, 0x80, 0]);
        let (_, tail): (T, _) = durable_session_v1::take(&suffixed).unwrap();
        assert_eq!(
            tail,
            &[0xff, 0x80, 0],
            "the caller owns the historical suffix rule: {name}"
        );
        for prefix in [0, 1, suffixed.len() - 4] {
            assert!(
                durable_session_v1::take::<T>(&suffixed[..prefix]).is_err(),
                "{name}: {prefix}"
            );
        }
    }
}

#[test]
fn original_session_corpus_is_immutable() {
    let manifest = include_str!("../fixtures/durable-session-v1/capture-files.tsv");
    let mut count = 0;
    for row in manifest.lines().skip(1) {
        let fields: Vec<_> = row.split('\t').collect();
        assert_eq!(fields.len(), 4);
        let bytes = fixture(fields[0]);
        assert_eq!(
            bytes.len(),
            fields[1].parse::<usize>().unwrap(),
            "{}",
            fields[0]
        );
        assert_eq!(
            blake3::hash(&bytes).to_hex().as_str(),
            fields[2],
            "{}",
            fields[0]
        );
        count += 1;
    }
    assert_eq!(count, 170);
}

#[test]
fn all_original_session_storage_rows_have_frozen_parity() {
    verify_rows::<CursorInput>("cursor-inputs.rows", 18);
    verify_rows::<CursorReceipt>("cursor-receipts.rows", 37);
    verify_rows::<CursorMetadata>("cursor-metadata.rows", 2);
    verify_rows::<LegacyCursorEnvelope>("legacy-cursor-envelopes.rows", 18);
    verify_rows::<CursorEnvelope>("cursor-envelopes.rows", 18);
    verify_rows::<MaintenanceEnvelope>("maintenance-envelopes.rows", 18);
    verify_rows::<ManagedCursorInput>("managed-cursor-inputs.rows", 18);
    verify_rows::<ManagedCursorEnvelope>("managed-cursor-envelopes.rows", 18);
    verify_rows::<RequestStreamEnvelope>("stream-envelopes.rows", 8);
    verify_rows::<SessionMembershipRequest>("membership-requests.rows", 4);
    verify_rows::<SessionMembershipReceipt>("membership-receipts.rows", 4);
    verify_rows::<MembershipContext>("membership-contexts.rows", 4);
    verify_rows::<MembershipState>("membership-states.rows", 5);
    verify_rows::<SessionPlacementRequest>("placement-requests.rows", 9);
    verify_rows::<PlacementRecord>("placement-records.rows", 9);
    verify_rows::<StoredPlacement>("stored-placements.rows", 9);
    verify_rows::<PlacementState>("placement-states.rows", 4);
    verify_rows::<crate::request_streams::StreamSlotData>("request-stream-slots.rows", 108);
    verify_rows::<RequestStreamsCheckpoint>("request-stream-checkpoints.rows", 3);
    verify_rows::<SnapshotEnvelope>("snapshot-ss1.rows", 2);
    verify_rows::<SnapshotEnvelopeV2>("snapshot-ss2.rows", 2);
    verify_rows::<SnapshotEnvelopeV3>("snapshot-ss3.rows", 5);
    verify_rows::<SnapshotEnvelopeV4>("snapshot-ss4.rows", 4);
    verify_rows::<SnapshotEnvelopeV5>("snapshot-ss5.rows", 3);
}

#[test]
fn original_metadata_envelopes_preserve_all_variants() {
    verify_envelopes::<LegacyCursorEnvelope>("cu1", LEGACY_CURSOR_MAGIC, 18);
    verify_envelopes::<CursorEnvelope>("cu2", CURSOR_MAGIC, 18);
    verify_envelopes::<MaintenanceEnvelope>("cm1", CURSOR_MAINTENANCE_MAGIC, 18);
    verify_envelopes::<ManagedCursorEnvelope>("mu1", MANAGED_CURSOR_MAGIC, 18);
    verify_envelopes::<RequestStreamEnvelope>("ms1", REQUEST_STREAM_MAGIC, 8);
    verify_envelopes::<MembershipContext>("mc1", MEMBERSHIP_MAGIC, 4);
    verify_envelopes::<PlacementRecord>("pl1", PLACEMENT_MAGIC, 9);
}

#[test]
fn original_snapshot_envelopes_preserve_empty_and_broad_nested_state() {
    verify_envelopes::<SnapshotEnvelope>("ss1", SNAPSHOT_MAGIC, 2);
    verify_envelopes::<SnapshotEnvelopeV2>("ss2", SNAPSHOT_V2_MAGIC, 2);
    verify_envelopes::<SnapshotEnvelopeV3>("ss3", SNAPSHOT_V3_MAGIC, 5);
    verify_envelopes::<SnapshotEnvelopeV4>("ss4", SNAPSHOT_V4_MAGIC, 4);
    verify_envelopes::<SnapshotEnvelopeV5>("ss5", SNAPSHOT_V5_MAGIC, 3);
}

#[test]
fn original_membership_placement_cursor_and_stream_hashes_match() {
    let inputs: Vec<CursorInput> = fixed("cursor-inputs.rows");
    let hashes: Vec<ContentHash> = fixed("cursor-operation-hashes.rows");
    assert_eq!(inputs.len(), hashes.len());
    for (input, hash) in inputs.iter().zip(hashes) {
        let operation =
            durable_session_v1::encode(&[], &input.command.operation, usize::MAX).unwrap();
        assert_eq!(ContentHash(*blake3::hash(&operation).as_bytes()), hash);
        assert_eq!(input.intent_hash, hash);
    }
    let requests: Vec<SessionMembershipRequest> = fixed("membership-requests.rows");
    let hashes: Vec<[u8; 32]> = fixed("membership-request-hashes.rows");
    for (request, hash) in requests.iter().zip(hashes) {
        assert_eq!(request.hash().unwrap(), hash);
    }
    let placements: Vec<StoredPlacement> = fixed("stored-placements.rows");
    for (index, placement) in placements.iter().enumerate() {
        let bytes = encode_placement(&placement.record).unwrap();
        assert_eq!(bytes, fixture(&format!("pl1-{index:02}.bin")));
        assert_eq!(
            ContentHash(*blake3::hash(&bytes).as_bytes()),
            placement.fence.record_hash
        );
        assert_eq!(
            focal_directory::placement_digest(&placement.record.request.placement).unwrap(),
            placement.fence.placement_digest
        );
    }
    let slots: Vec<crate::request_streams::StreamSlotData> = fixed("request-stream-slots.rows");
    let receipts = slots.iter().find(|slot| slot.rows.len() == 98).unwrap();
    let hashes: Vec<ContentHash> = fixed("managed-receipt-hashes.rows");
    for (receipt, hash) in receipts.rows.iter().zip(hashes) {
        assert_eq!(receipt.content_hash().unwrap(), hash);
    }
    let streams: Vec<RequestStreamEnvelope> = fixed("stream-envelopes.rows");
    let hashes: Vec<ContentHash> = fixed("stream-control-hashes.rows");
    for (stream, hash) in streams.iter().zip(hashes) {
        assert_eq!(stream.input.intent_hash().unwrap(), hash);
    }
}

fn live_checkpoint(session: &mut Session, version: u8) {
    let bytes = session.encode_checkpoint(false).unwrap().unwrap().bytes;
    assert_eq!(
        bytes,
        fixture(&format!("live-ss{version}.bin")),
        "SS{version} actual writer"
    );
    assert_eq!(
        session.core.encode_checkpoint().unwrap(),
        fixture(&format!("live-ss{version}.core"))
    );
    session.audit_graph().unwrap();
}
fn original_prefix(session: &mut Session) {
    elect(session);
    let request: AuthenticatedInput = fixed("live-epoch-input.bin");
    session.submit_local(&request).unwrap();
    let cursor: CursorInput = fixed("live-cursor-input.bin");
    session.submit_cursor_local(&cursor).unwrap();
}
fn original_placement(session: &mut Session) {
    for (kind, operation) in [
        (SessionFenceKind::Created, 601),
        (SessionFenceKind::Cutover, 602),
        (SessionFenceKind::Activated, 602),
    ] {
        let request = placement_request(session, kind, operation);
        session.propose_placement(&request).unwrap();
        session.poll().unwrap();
        assert!(session.placement_receipt(&request).unwrap().is_some());
    }
}
fn original_managed_tail(session: &mut Session) {
    let stream = stream_register(session, 0, 4);
    let domain: ManagedAuthenticatedInput = fixed("live-managed-input.bin");
    assert_eq!(domain.key.stream, stream);
    assert_eq!(
        commit_managed(session, &domain),
        fixed::<ManagedReceipt>("live-managed-receipt.bin")
    );
    let cursor: ManagedCursorInput = fixed("live-managed-cursor-input.bin");
    session.propose_managed_cursor(&cursor, false).unwrap();
    session.poll().unwrap();
    let seal: RequestStreamControlInput = fixed("live-seal-input.bin");
    session.propose_request_stream(&seal).unwrap();
    session.poll().unwrap();
    assert!(session.request_stream_receipt(&seal).unwrap().is_some());
    assert_eq!(
        durable_session_v1::encode(&[], &session.request_streams.checkpoint(), usize::MAX).unwrap(),
        fixture("live-stream-state.bin")
    );
}

#[test]
fn actual_original_history_writes_identical_ss3_ss4_ss5_and_recovers() {
    let directory = tempfile::tempdir().unwrap();
    let mut session = Session::open(
        directory.path(),
        identity(),
        config(),
        SessionLimits::default(),
    )
    .unwrap();
    original_prefix(&mut session);
    live_checkpoint(&mut session, 3);
    original_placement(&mut session);
    live_checkpoint(&mut session, 4);
    original_managed_tail(&mut session);
    live_checkpoint(&mut session, 5);
    session.checkpoint().unwrap();
    drop(session);
    let mut session = Session::open(
        directory.path(),
        identity(),
        config(),
        SessionLimits::default(),
    )
    .unwrap();
    assert_eq!(
        session.core.encode_checkpoint().unwrap(),
        fixture("live-ss5.core")
    );
    assert_eq!(
        durable_session_v1::encode(&[], &session.request_streams.checkpoint(), usize::MAX).unwrap(),
        fixture("live-stream-state.bin")
    );
    elect(&mut session);
    let domain: ManagedAuthenticatedInput = fixed("live-managed-input.bin");
    let ManagedSubmission::Committed(receipt) = session.propose_managed(&domain).unwrap() else {
        panic!("the original recovered ordinal must return its committed receipt");
    };
    assert_eq!(
        *receipt,
        fixed::<ManagedReceipt>("live-managed-receipt.bin")
    );
    // A fresh committed tail must still recover after the frozen snapshot.
    session.submit_local(&epoch(2)).unwrap();
    let after = session.core.encode_checkpoint().unwrap();
    drop(session);
    let session = Session::open(
        directory.path(),
        identity(),
        config(),
        SessionLimits::default(),
    )
    .unwrap();
    assert_eq!(session.core.encode_checkpoint().unwrap(), after);
    session.audit_graph().unwrap();
}

fn snapshot_context(version: u8) -> (u64, u64, MembershipConfiguration) {
    // This test-only tuple is capture metadata, not a persistent codec contract.
    postcard::from_bytes(&fixture(&format!("live-ss{}.context", version.max(3)))).unwrap()
}
#[test]
fn original_ss1_through_ss5_restore_replay_tail_and_reject_suffixes_without_publication() {
    for version in 1..=5 {
        let directory = tempfile::tempdir().unwrap();
        let mut session = Session::open(
            directory.path(),
            identity(),
            config(),
            SessionLimits::default(),
        )
        .unwrap();
        if version == 5 {
            elect(&mut session);
            durable_managed_support(&mut session);
        }
        let (index, term, configuration) = snapshot_context(version);
        let bytes = fixture(&format!("live-ss{version}.bin"));
        session
            .restore_snapshot(&bytes, index, term, &configuration)
            .unwrap();
        let expected_core = fixture(&format!("live-ss{}.core", version.max(3)));
        assert_eq!(session.core.encode_checkpoint().unwrap(), expected_core);
        let before = durable_session_v1::snapshot(&session, &expected_core).unwrap();
        let malformed = fixture(&format!("reader-ss{version}-trailing.bin"));
        assert!(matches!(
            session.restore_snapshot(&malformed, index, term, &configuration),
            Err(LedgerError::Corrupt)
        ));
        assert_eq!(session.core.encode_checkpoint().unwrap(), expected_core);
        assert_eq!(
            durable_session_v1::snapshot(&session, &expected_core).unwrap(),
            before
        );
        session.audit_graph().unwrap();
        // Exercise real DeltaSource replay over the recovered original tail.
        let mut replayed = Vec::new();
        let end = session
            .replay(
                Position::resolved(identity(), session.stream_bounds().floor),
                ReplayLimit::default(),
                &mut |delta| {
                    replayed.push(delta.clone());
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(end, Position::resolved(identity(), session.sequence()));
        let expected: Vec<Delta> = match version {
            1 => Vec::new(),
            2 => {
                durable_session_v1::take::<SnapshotEnvelopeV2>(&bytes[SNAPSHOT_V2_MAGIC.len()..])
                    .unwrap()
                    .0
                    .deltas
            }
            3 => {
                durable_session_v1::take::<SnapshotEnvelopeV3>(&bytes[SNAPSHOT_V3_MAGIC.len()..])
                    .unwrap()
                    .0
                    .state
                    .deltas
            }
            4 => {
                durable_session_v1::take::<SnapshotEnvelopeV4>(&bytes[SNAPSHOT_V4_MAGIC.len()..])
                    .unwrap()
                    .0
                    .state
                    .state
                    .deltas
            }
            5 => {
                durable_session_v1::take::<SnapshotEnvelopeV5>(&bytes[SNAPSHOT_V5_MAGIC.len()..])
                    .unwrap()
                    .0
                    .state
                    .state
                    .state
                    .deltas
            }
            _ => unreachable!(),
        };
        assert_eq!(replayed, expected);
    }
}

#[test]
fn historical_cursor_and_maintenance_suffixes_replay_identical_original_receipts() {
    for version in 1..=2 {
        let directory = tempfile::tempdir().unwrap();
        let mut session = Session::open(
            directory.path(),
            identity(),
            config(),
            SessionLimits::default(),
        )
        .unwrap();
        elect(&mut session);
        session.submit_local(&epoch(1)).unwrap();
        let bytes = fixture(&format!("reader-cu{version}-trailing.bin"));
        let (receipt, _charge) = session.apply_cursor_entry(&bytes, 999).unwrap();
        assert_eq!(
            receipt,
            fixed::<CursorReceipt>(&format!("reader-cu{version}-accepted.bin"))
        );
    }
    let directory = tempfile::tempdir().unwrap();
    let mut session = Session::open(
        directory.path(),
        identity(),
        config(),
        SessionLimits::default(),
    )
    .unwrap();
    elect(&mut session);
    session.submit_local(&epoch(1)).unwrap();
    let input = register(&session, 802, false);
    session.submit_cursor_local(&input).unwrap();
    session
        .apply_maintenance_entry(&fixture("reader-cm1-trailing.bin"))
        .unwrap();
    assert_eq!(
        session.cursors.checkpoint(),
        &fixed::<CursorCheckpoint>("reader-cm1-accepted.bin")
    );
}

#[test]
fn exact_metadata_readers_reject_committed_suffix_before_application_publication() {
    // Membership's corresponding actual-reader check is in membership_tests.
    for family in [PLACEMENT_MAGIC, MANAGED_CURSOR_MAGIC, REQUEST_STREAM_MAGIC] {
        let directory = tempfile::tempdir().unwrap();
        let mut session = Session::open(
            directory.path(),
            identity(),
            config(),
            SessionLimits::default(),
        )
        .unwrap();
        elect(&mut session);
        session.submit_local(&epoch(1)).unwrap();
        if family == PLACEMENT_MAGIC {
            let request = placement_request(&session, SessionFenceKind::Created, 901);
            session.propose_placement(&request).unwrap();
        } else {
            let stream = stream_register(&mut session, 0, 4);
            if family == MANAGED_CURSOR_MAGIC {
                let old = register(&session, 902, false);
                let request = ManagedCursorInput {
                    key: ManagedRequestKey {
                        stream,
                        ordinal: 1,
                        id: old.key.id,
                    },
                    intent_hash: old.intent_hash,
                    command: old.command,
                };
                assert!(matches!(
                    session.propose_managed_cursor(&request, false).unwrap(),
                    ManagedSubmission::Pending(_)
                ));
            } else {
                let request = stream_control(
                    903,
                    RequestStreamCommand::Register {
                        slot: 1,
                        expected_generation: 0,
                        owner: RequestId::from_u128(904),
                        window: 4,
                    },
                );
                assert!(matches!(
                    session.propose_request_stream(&request).unwrap(),
                    RequestStreamSubmission::Pending(_)
                ));
            }
        }
        let mut events = session.consensus.drain().unwrap();
        assert_eq!(events.committed.len(), 1);
        assert!(events.committed[0].data.starts_with(family));
        let core = session.core.encode_checkpoint().unwrap();
        let before = durable_session_v1::snapshot(&session, &core).unwrap();
        events.committed[0].data.extend_from_slice(&[0xff, 0x80, 0]);
        assert!(matches!(
            session.apply_events(events),
            Err(LedgerError::Corrupt)
        ));
        assert_eq!(session.core.encode_checkpoint().unwrap(), core);
        assert_eq!(
            durable_session_v1::snapshot(&session, &core).unwrap(),
            before
        );
        session.audit_graph().unwrap();
    }
}

#[test]
fn original_managed_cursor_size_fences_proposal_and_actual_committed_replay() {
    let bytes = fixture("live-managed-cursor-input.bin");
    assert_eq!(bytes.len(), 233);
    for replay_limit in [bytes.len() - 1, bytes.len()] {
        let directory = tempfile::tempdir().unwrap();
        let mut session = Session::open(
            directory.path(),
            identity(),
            config(),
            SessionLimits::default(),
        )
        .unwrap();
        original_prefix(&mut session);
        original_placement(&mut session);
        let stream = stream_register(&mut session, 0, 4);
        let domain: ManagedAuthenticatedInput = fixed("live-managed-input.bin");
        assert_eq!(stream, domain.key.stream);
        commit_managed(&mut session, &domain);
        let cursor: ManagedCursorInput = fixed("live-managed-cursor-input.bin");
        let core = session.core.encode_checkpoint().unwrap();
        let before = durable_session_v1::snapshot(&session, &core).unwrap();
        session.limits.core.max_command_bytes = bytes.len() - 1;
        assert!(matches!(
            session.propose_managed_cursor(&cursor, false),
            Err(LedgerError::Capacity)
        ));
        assert_eq!(
            durable_session_v1::snapshot(&session, &core).unwrap(),
            before
        );
        session.limits.core.max_command_bytes = bytes.len();
        assert!(matches!(
            session.propose_managed_cursor(&cursor, false).unwrap(),
            ManagedSubmission::Pending(_)
        ));
        let events = session.consensus.drain().unwrap();
        assert_eq!(events.committed.len(), 1);
        assert!(events.committed[0].data.starts_with(MANAGED_CURSOR_MAGIC));
        // A different replica/restart has no local candidate. The same original
        // input must pass the exact frozen byte fence during committed replay.
        session.pending_managed = None;
        session.limits.core.max_command_bytes = replay_limit;
        let result = session.apply_events(events);
        if replay_limit < bytes.len() {
            assert!(matches!(result, Err(LedgerError::Capacity)));
            assert_eq!(
                durable_session_v1::snapshot(&session, &core).unwrap(),
                before
            );
        } else {
            let result = result.unwrap();
            assert_eq!(result.managed_committed.len(), 1);
            let expected: RequestStreamsCheckpoint = fixed("live-stream-state.bin");
            let expected = expected
                .slots
                .iter()
                .flat_map(|slot| &slot.rows)
                .find(|receipt| receipt.key == cursor.key)
                .unwrap();
            assert_eq!(&result.managed_committed[0].receipt, expected);
        }
        assert_eq!(session.core.encode_checkpoint().unwrap(), core);
        session.audit_graph().unwrap();
    }
}

fn original_delta_body_lengths(bytes: &[u8], magic: &[u8]) -> Vec<usize> {
    // Walk the captured SS2 field order and measure consumed ORIGINAL bytes;
    // expected lengths do not come from any current serialization/sizing call.
    let (_, rest): (u16, _) = durable_session_v1::take(bytes.strip_prefix(magic).unwrap()).unwrap();
    let (_, rest): (LedgerId, _) = durable_session_v1::take(rest).unwrap();
    let (_, rest): (u64, _) = durable_session_v1::take(rest).unwrap();
    let (_, rest): (Vec<u8>, _) = durable_session_v1::take(rest).unwrap();
    let (_, rest): (CursorCheckpoint, _) = durable_session_v1::take(rest).unwrap();
    let (_, rest): (CursorMetadata, _) = durable_session_v1::take(rest).unwrap();
    let (_, rest): (SessionSeq, _) = durable_session_v1::take(rest).unwrap();
    let (count, mut rest): (usize, _) = postcard::take_from_bytes(rest).unwrap();
    let mut sizes = Vec::new();
    for _ in 0..count {
        let (_, tail): (Delta, _) = durable_session_v1::take(rest).unwrap();
        sizes.push(rest.len() - tail.len());
        rest = tail;
    }
    sizes
}

#[test]
fn original_delta_byte_budget_and_retention_cut_survive_snapshot_restore() {
    let bytes = fixture("live-ss5.bin");
    let lengths = original_delta_body_lengths(&bytes, SNAPSHOT_V5_MAGIC);
    assert!(!lengths.is_empty());
    let total: usize = lengths.iter().sum();
    let (index, term, configuration) = snapshot_context(5);
    for limit in [total - 1, total] {
        let directory = tempfile::tempdir().unwrap();
        let limits = SessionLimits {
            delta_bytes: limit,
            ..SessionLimits::default()
        };
        let mut session = Session::open(directory.path(), identity(), config(), limits).unwrap();
        elect(&mut session);
        durable_managed_support(&mut session);
        let before_core = session.core.encode_checkpoint().unwrap();
        let before = durable_session_v1::snapshot(&session, &before_core).unwrap();
        let result = session.restore_snapshot(&bytes, index, term, &configuration);
        if limit < total {
            assert!(matches!(result, Err(LedgerError::Capacity)));
            assert_eq!(session.core.encode_checkpoint().unwrap(), before_core);
            assert_eq!(
                durable_session_v1::snapshot(&session, &before_core).unwrap(),
                before
            );
        } else {
            result.unwrap();
            assert_eq!(session.delta_bytes, total);
            assert_eq!(
                session
                    .deltas
                    .iter()
                    .map(|row| row.bytes)
                    .collect::<Vec<_>>(),
                lengths
            );
            let before_floor = session.stream_bounds().floor;
            assert_eq!(
                session
                    .retention_forecast_from(&[], session.sequence(), std::iter::empty())
                    .unwrap(),
                before_floor
            );
            session.limits.delta_bytes = total - 1;
            let first_sequence = session.deltas.front().unwrap().delta.id.sequence;
            assert!(first_sequence > before_floor);
            assert_eq!(
                session
                    .retention_forecast_from(&[], session.sequence(), std::iter::empty())
                    .unwrap(),
                first_sequence
            );
            // Planning the necessary retirement never publishes it by itself.
            assert_eq!(session.stream_bounds().floor, before_floor);
            assert_eq!(session.delta_bytes, total);
        }
        session.audit_graph().unwrap();
    }
}
