#[test]
fn v1_legacy_entry_suffix_rejects_entire_committed_epoch_before_publication() {
    let dir = tempfile::tempdir().unwrap();
    let mut session =
        Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
    elect(&mut session);
    for request in independent_epoch_requests(2) {
        session.propose(&request).unwrap();
    }
    let before = session.core.encode_checkpoint().unwrap();
    let mut events = session.consensus.drain().unwrap();
    assert_eq!(events.committed.len(), 2);
    for entry in &events.committed {
        let body = entry.data.strip_prefix(ENTRY_MAGIC).unwrap();
        let prepared = PreparedMutation::decode_v1(body).unwrap();
        assert_eq!(postcard::to_stdvec(&prepared).unwrap(), body);
    }
    events.committed[1].data.push(0);
    assert!(matches!(
        session.apply_events(events),
        Err(LedgerError::Core(CoreError::Codec(_)))
    ));
    assert_eq!(session.core.encode_checkpoint().unwrap(), before);
    assert_eq!(session.graph_sequence(), SessionSeq(0));
}

#[test]
fn v1_managed_entry_suffix_cannot_publish_core_graph_or_stream_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let mut session =
        Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
    elect(&mut session);
    let stream = stream_register(&mut session, 0, 2);
    let request = managed_claim(stream, 1);
    session.propose_managed(&request).unwrap();
    let before = session.core.encode_checkpoint().unwrap();
    let stream_before = session
        .request_streams
        .state(stream.principal, stream.slot)
        .unwrap();
    let mut events = session.consensus.drain().unwrap();
    assert_eq!(events.committed.len(), 1);
    let body = events.committed[0]
        .data
        .strip_prefix(MANAGED_DOMAIN_MAGIC)
        .unwrap();
    let prepared = focal_core::PreparedManagedMutation::decode_v1(body).unwrap();
    assert_eq!(postcard::to_stdvec(&prepared).unwrap(), body);
    // Changed bytes also force actual committed decoding rather than reuse of
    // the local prepared candidate, as on another replica or after restart.
    events.committed[0].data.push(0);
    assert!(matches!(
        session.apply_events(events),
        Err(LedgerError::Core(CoreError::Codec(_)))
    ));
    assert_eq!(session.core.encode_checkpoint().unwrap(), before);
    assert_eq!(session.graph_sequence(), SessionSeq(0));
    assert_eq!(
        session
            .request_streams
            .state(stream.principal, stream.slot)
            .unwrap(),
        stream_before
    );
    assert!(
        session
            .managed_receipt(
                &request.key,
                managed_command_hash(&request).unwrap(),
                ManagedRequestFamily::Domain
            )
            .unwrap()
            .is_none()
    );
}

#[test]
fn v1_inner_checkpoint_suffix_is_rejected_by_real_session_restore() {
    let dir = tempfile::tempdir().unwrap();
    let mut session =
        Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
    elect(&mut session);
    session
        .submit_local(&input(
            1,
            Command::NegotiateEpoch {
                epoch: RequestEpoch(1),
            },
        ))
        .unwrap();
    let encoded = session.encode_checkpoint(false).unwrap().unwrap();
    let body = encoded.bytes.strip_prefix(SNAPSHOT_V3_MAGIC).unwrap();
    let mut snapshot: SnapshotEnvelopeV3 = postcard::from_bytes(body).unwrap();
    let core_bytes = &mut snapshot.state.core;
    let mut payload = core_bytes[40..].to_vec();
    payload.push(0);
    core_bytes.truncate(8);
    core_bytes.extend_from_slice(blake3::hash(&payload).as_bytes());
    core_bytes.extend_from_slice(&payload);
    let mut bytes = SNAPSHOT_V3_MAGIC.to_vec();
    bytes.extend(postcard::to_stdvec(&snapshot).unwrap());
    let before = session.core.encode_checkpoint().unwrap();
    let configuration = session.consensus.membership_configuration();
    let term = session.status().term;
    assert!(matches!(
        session.restore_snapshot(&bytes, session.applied_raft, term, &configuration),
        Err(LedgerError::Core(CoreError::Codec(_)))
    ));
    assert_eq!(session.core.encode_checkpoint().unwrap(), before);
    session.audit_graph().unwrap();
}

#[test]
fn original_session_snapshot_envelopes_reject_suffixes_and_recover_exact_core() {
    for schema in [1u16, 2] {
        let dir = tempfile::tempdir().unwrap();
        let mut session =
            Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
        elect(&mut session);
        let request = input(1, Command::NegotiateEpoch { epoch: RequestEpoch(1) });
        let receipt = session.submit_local(&request).unwrap();
        let encoded = session.encode_checkpoint(false).unwrap().unwrap();
        let current: SnapshotEnvelopeV3 =
            postcard::from_bytes(encoded.bytes.strip_prefix(SNAPSHOT_V3_MAGIC).unwrap()).unwrap();
        let mut bytes = if schema == 1 {
            let old = SnapshotEnvelope {
                schema: 1,
                ledger: identity(),
                raft_index: session.applied_raft,
                core: current.state.core.clone(),
            };
            let mut bytes = SNAPSHOT_MAGIC.to_vec();
            bytes.extend(postcard::to_stdvec(&old).unwrap());
            bytes
        } else {
            let mut bytes = SNAPSHOT_V2_MAGIC.to_vec();
            bytes.extend(postcard::to_stdvec(&current.state).unwrap());
            bytes
        };
        let before = session.core.encode_checkpoint().unwrap();
        let configuration = session.consensus.membership_configuration();
        let term = session.status().term;
        bytes.push(0);
        assert!(matches!(session.restore_snapshot(&bytes, session.applied_raft, term, &configuration), Err(LedgerError::Corrupt)));
        assert_eq!(session.core.encode_checkpoint().unwrap(), before);
        session.audit_graph().unwrap();
        bytes.pop();
        session.restore_snapshot(&bytes, session.applied_raft, term, &configuration).unwrap();
        assert_eq!(session.core.encode_checkpoint().unwrap(), before);
        assert_eq!(session.submit_local(&request).unwrap(), receipt);
        session.audit_graph().unwrap();
    }
}
