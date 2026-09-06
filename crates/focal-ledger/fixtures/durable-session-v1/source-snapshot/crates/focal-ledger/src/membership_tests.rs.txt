fn membership_request(
    session: &Session,
    id: u8,
    change: MembershipChange,
) -> SessionMembershipRequest {
    let view = session.membership().unwrap();
    SessionMembershipRequest {
        id: [id; 16],
        expected_index: view.configuration_index,
        expected: view.configuration,
        change,
    }
}
#[test]
fn membership_exact_receipt_survives_wal_replay_and_snapshot_migration() {
    for checkpoint in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut session =
            Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
        elect(&mut session);
        let request = membership_request(&session, 1, MembershipChange::AddLearner { node: 2 });
        session.propose_membership(&request).unwrap();
        assert_eq!(session.pending_count(), 1);
        assert!(session.membership_receipt(&request).unwrap().is_none());
        session.propose_membership(&request).unwrap();
        for _ in 0..4 {
            session.poll().unwrap();
        }
        let receipt = session
            .membership_receipt(&request)
            .unwrap()
            .unwrap()
            .clone();
        assert_eq!(receipt.configuration.learners, vec![2]);
        assert_eq!(session.pending_count(), 0);
        assert_eq!(session.sequence(), SessionSeq(0));
        if checkpoint {
            session.checkpoint().unwrap();
        }
        drop(session);
        let mut reopened =
            Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
        elect(&mut reopened);
        assert_eq!(
            reopened.membership_receipt(&request).unwrap(),
            Some(&receipt)
        );
        reopened.propose_membership(&request).unwrap();
        let removal = membership_request(&reopened, 2, MembershipChange::Remove { node: 2 });
        reopened.propose_membership(&removal).unwrap();
        for _ in 0..4 {
            reopened.poll().unwrap();
        }
        assert!(
            reopened
                .membership()
                .unwrap()
                .configuration
                .learners
                .is_empty()
        );
        assert!(matches!(
            reopened.propose_membership(&request),
            Err(LedgerError::MembershipConflict)
        ));
        let removal_receipt = reopened
            .membership_receipt(&removal)
            .unwrap()
            .unwrap()
            .clone();
        drop(reopened);
        // The checkpoint can contain a learner that a later committed suffix
        // removes. Validate it against its own Raft configuration prefix.
        let mut with_suffix =
            Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
        elect(&mut with_suffix);
        assert_eq!(
            with_suffix.membership_receipt(&removal).unwrap(),
            Some(&removal_receipt)
        );
        assert!(
            with_suffix
                .membership()
                .unwrap()
                .configuration
                .learners
                .is_empty()
        );
    }
}
#[test]
fn membership_conflicting_identity_and_uncaught_learner_never_publish() {
    let dir = tempfile::tempdir().unwrap();
    let mut session =
        Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
    elect(&mut session);
    let request = membership_request(&session, 1, MembershipChange::AddLearner { node: 2 });
    session.propose_membership(&request).unwrap();
    let mut conflicting = request.clone();
    conflicting.change = MembershipChange::AddLearner { node: 3 };
    assert!(matches!(
        session.propose_membership(&conflicting),
        Err(LedgerError::MembershipConflict)
    ));
    for _ in 0..4 {
        session.poll().unwrap();
    }
    let view = session.membership().unwrap();
    assert!(matches!(
        session.propose_membership(&conflicting),
        Err(LedgerError::MembershipConflict)
    ));
    let promotion = membership_request(&session, 2, MembershipChange::Promote { node: 2 });
    assert!(matches!(
        session.propose_membership(&promotion),
        Err(LedgerError::Consensus(ConsensusError::LearnerBehind))
    ));
    assert_eq!(session.membership().unwrap(), view);
}

#[test]
fn membership_context_and_checkpoint_reject_trailing_bytes_and_wrong_scope() {
    let dir = tempfile::tempdir().unwrap();
    let mut session =
        Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
    elect(&mut session);
    let request = membership_request(&session, 1, MembershipChange::AddLearner { node: 2 });
    let context = MembershipContext {
        ledger: identity(),
        id: request.id,
        expected_index: request.expected_index,
        change: request.change,
        hash: request.hash().unwrap(),
    };
    let mut encoded = MEMBERSHIP_MAGIC.to_vec();
    encoded.extend(postcard::to_stdvec(&context).unwrap());
    let applied = focal_consensus::AppliedMembership {
        index: 2,
        term: 1,
        context: encoded,
        before: request.expected.clone(),
        after: request.change.apply_to(&request.expected).unwrap(),
    };
    let mut trailing = applied.clone();
    trailing.context.push(0);
    assert!(matches!(
        session.apply_membership(trailing),
        Err(LedgerError::Corrupt)
    ));
    let wrong_context = MembershipContext {
        ledger: LedgerId {
            tenant: TenantId::from_u128(99),
            ..identity()
        },
        ..context
    };
    let mut wrong = applied.clone();
    wrong.context = MEMBERSHIP_MAGIC.to_vec();
    wrong
        .context
        .extend(postcard::to_stdvec(&wrong_context).unwrap());
    assert!(matches!(
        session.apply_membership(wrong),
        Err(LedgerError::Corrupt)
    ));
    assert_eq!(session.membership().unwrap().configuration_index, 0);
    let envelope = SnapshotEnvelopeV3 {
        state: SnapshotEnvelopeV2 {
            schema: 2,
            ledger: identity(),
            raft_index: 1,
            core: session.core.encode_checkpoint().unwrap(),
            cursors: session.cursors.checkpoint().clone(),
            cursor_meta: CursorMetadata::default(),
            delta_floor: SessionSeq(0),
            deltas: vec![],
        },
        membership: MembershipState {
            configuration_index: 2,
            latest: None,
        },
    };
    let mut snapshot = SNAPSHOT_V3_MAGIC.to_vec();
    snapshot.extend(postcard::to_stdvec(&envelope).unwrap());
    assert!(matches!(
        session.restore_snapshot(
            &snapshot,
            1,
            1,
            &session.consensus.membership_configuration()
        ),
        Err(LedgerError::Corrupt)
    ));
    snapshot.push(0);
    assert!(matches!(
        session.restore_snapshot(
            &snapshot,
            1,
            1,
            &session.consensus.membership_configuration()
        ),
        Err(LedgerError::Corrupt)
    ));
}

#[test]
fn checksum_valid_checkpoint_cannot_forge_membership_configuration_or_future_term() {
    for future_term in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut session =
            Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
        elect(&mut session);
        let request = membership_request(&session, 1, MembershipChange::AddLearner { node: 2 });
        session.propose_membership(&request).unwrap();
        for _ in 0..4 {
            session.poll().unwrap();
        }
        let mut receipt = session.membership_state.latest.clone().unwrap();
        if future_term {
            receipt.term = session.status().term + 1;
        } else {
            receipt.configuration.learners = vec![3];
        }
        let envelope = SnapshotEnvelopeV3 {
            state: SnapshotEnvelopeV2 {
                schema: 2,
                ledger: identity(),
                raft_index: session.applied_raft,
                core: session.core.encode_checkpoint().unwrap(),
                cursors: session.cursors.checkpoint().clone(),
                cursor_meta: CursorMetadata::default(),
                delta_floor: SessionSeq(0),
                deltas: vec![],
            },
            membership: MembershipState {
                configuration_index: receipt.index,
                latest: Some(receipt),
            },
        };
        let mut bytes = SNAPSHOT_V3_MAGIC.to_vec();
        bytes.extend(postcard::to_stdvec(&envelope).unwrap());
        // The real Raft/WAL adapter seals this malformed application snapshot
        // with valid checksums. Only cross-layer prefix validation can reject it.
        session
            .consensus
            .checkpoint(session.applied_raft, bytes)
            .unwrap();
        drop(session);
        assert!(matches!(
            Session::open(dir.path(), identity(), config(), SessionLimits::default()),
            Err(LedgerError::Corrupt)
        ));
    }
}
