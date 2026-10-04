use focal_stream::{CursorMode, CursorToken, DeltaFilter};

fn epoch(n: u64) -> AuthenticatedInput {
    let mut request = input(
        100 + u128::from(n),
        Command::NegotiateEpoch {
            epoch: RequestEpoch(n),
        },
    );
    request.request_epoch = RequestEpoch(n);
    request
}
fn cursor_input(s: &Session, id: u128, operation: CursorOperation) -> CursorInput {
    CursorInput {
        ledger: identity(),
        key: RequestKey {
            principal: ParticipantId::from_u128(1),
            epoch: RequestEpoch(1),
            id: RequestId::from_u128(id),
        },
        intent_hash: ContentHash(
            *blake3::hash(&postcard::to_stdvec(&operation).unwrap()).as_bytes(),
        ),
        command: CursorCommand {
            expected_revision: s.cursor_revision(),
            now: s.cursor_clock(),
            operation,
        },
    }
}
fn register(s: &Session, id: u128, protected: bool) -> CursorInput {
    let consumer = ConsumerId::from_u128(1);
    let scope = ContentHash([8; 32]);
    let filter = DeltaFilter::All;
    let start = Position::origin(identity());
    cursor_input(
        s,
        id,
        if protected {
            CursorOperation::RegisterProtected {
                consumer,
                scope,
                filter,
                start,
            }
        } else {
            CursorOperation::Register {
                consumer,
                scope,
                filter,
                start,
                expires_at: 1000,
            }
        },
    )
}
fn ack(s: &Session, id: u128, position: Position) -> CursorInput {
    let token = CursorToken {
        position,
        ..s.cursor(ConsumerId::from_u128(1)).unwrap().token
    };
    cursor_input(s, id, CursorOperation::Acknowledge { token })
}
fn cursor_receipt(submitted: CursorSubmission) -> CursorReceipt {
    match submitted {
        CursorSubmission::Committed(receipt) => *receipt,
        other => panic!("{other:?}"),
    }
}
fn replay_ids(s: &Session, from: Position, limit: ReplayLimit) -> (Position, Vec<DeltaId>) {
    let mut ids = Vec::new();
    let position = s
        .replay(from, limit, &mut |d| {
            ids.push(d.id);
            Ok(())
        })
        .unwrap();
    (position, ids)
}

#[test]
fn cursor_lost_ack_reply_recovers_exact_outcome_and_checkpoint_tail() {
    let dir = tempfile::tempdir().unwrap();
    let original;
    let request;
    let retained;
    {
        let mut s =
            Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
        elect(&mut s);
        s.submit_local(&epoch(1)).unwrap();
        let registration = register(&s, 1, true);
        s.submit_cursor_control_local(&registration).unwrap();
        s.submit_local(&epoch(2)).unwrap();
        request = ack(&s, 2, Position::resolved(identity(), SessionSeq(1)));
        assert!(matches!(
            s.submit_cursor(&request).unwrap(),
            CursorSubmission::Pending(_)
        ));
        let event = s.poll().unwrap();
        assert_eq!(event.cursor_committed.len(), 1);
        original = event.cursor_committed[0].clone();
        drop(event); // durable commit succeeded; response never reached consumer
        assert_eq!(original.domain_sequence, SessionSeq(2));
        assert_eq!(s.sequence(), SessionSeq(2));
        let later = ack(&s, 3, Position::resolved(identity(), SessionSeq(2)));
        s.submit_cursor_local(&later).unwrap();
        s.submit_local(&epoch(3)).unwrap();
        retained = replay_ids(
            &s,
            Position::resolved(identity(), SessionSeq(2)),
            ReplayLimit::default(),
        );
        s.checkpoint().unwrap();
    }
    let mut s = Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
    elect(&mut s);
    assert_eq!(s.cursor_revision(), 3);
    assert_eq!(
        replay_ids(
            &s,
            Position::resolved(identity(), SessionSeq(2)),
            ReplayLimit::default()
        ),
        retained
    );
    let mut retry = request.clone();
    retry.command.now = 100;
    retry.command.expected_revision = s.cursor_revision();
    assert_eq!(
        cursor_receipt(s.submit_cursor_local(&retry).unwrap()),
        original
    );
    assert_eq!(
        s.cursor(ConsumerId::from_u128(1)).unwrap().token.position,
        Position::resolved(identity(), SessionSeq(2))
    );
    let mut conflicting = retry;
    conflicting.intent_hash = ContentHash([77; 32]);
    assert!(matches!(
        s.submit_cursor(&conflicting),
        Err(LedgerError::CursorRequest(ErrorCode::IdempotencyConflict))
    ));
}

#[test]
fn retention_pressure_pauses_before_proposal_and_resolved_ack_unblocks() {
    let dir = tempfile::tempdir().unwrap();
    let limits = SessionLimits {
        delta_items: 2,
        ..Default::default()
    };
    let mut s = Session::open(dir.path(), identity(), config(), limits).unwrap();
    elect(&mut s);
    s.submit_local(&epoch(1)).unwrap();
    let register = register(&s, 1, true);
    assert!(matches!(
        s.submit_cursor(&register),
        Err(LedgerError::CursorRequest(ErrorCode::WrongActor))
    ));
    s.submit_cursor_control_local(&register).unwrap();
    s.submit_local(&epoch(2)).unwrap();
    let committed = s.status().committed_index;
    let baseline = s.memory_stats().used;
    assert!(matches!(
        s.propose(&epoch(3)),
        Err(LedgerError::Stream(StreamError::RetentionPinned {
            allowed_through: SessionSeq(0)
        }))
    ));
    assert_eq!(s.status().committed_index, committed);
    assert_eq!(s.pending_count(), 0);
    assert_eq!(s.memory_stats().used, baseline);
    let first = s.deltas.front().unwrap().delta.id;
    let partial = ack(&s, 2, Position::after_delta(first));
    s.submit_cursor_local(&partial).unwrap();
    assert!(matches!(
        s.propose(&epoch(3)),
        Err(LedgerError::Stream(StreamError::RetentionPinned { .. }))
    ));
    let mut resolved = ack(&s, 3, Position::resolved(identity(), SessionSeq(1)));
    resolved.command.now = u64::MAX; // protected retention ignores wall-clock expiry
    s.submit_cursor_local(&resolved).unwrap();
    s.submit_local(&epoch(3)).unwrap();
    assert_eq!(s.stream_bounds().floor, SessionSeq(1));
    assert_eq!(s.deltas.len(), 2);
    assert_eq!(
        s.cursor(ConsumerId::from_u128(1)).unwrap().mode,
        CursorMode::Protected
    );
    s.checkpoint().unwrap();
    drop(s);
    let s = Session::open(
        dir.path(),
        identity(),
        config(),
        SessionLimits {
            delta_items: 2,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(s.stream_bounds().floor, SessionSeq(1));
    assert_eq!(
        replay_ids(
            &s,
            Position::resolved(identity(), SessionSeq(1)),
            ReplayLimit::default()
        )
        .1
        .len(),
        2
    );
}

#[test]
fn cursor_and_domain_share_request_keys_epoch_floor_and_owner_fencing() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
    elect(&mut s);
    let registration = register(&s, 1, false);
    assert!(matches!(
        s.submit_cursor(&registration),
        Err(LedgerError::CursorRequest(
            ErrorCode::RequestEpochNotAdmitted
        ))
    ));
    s.submit_local(&epoch(1)).unwrap();
    let mut used_domain = register(&s, 101, false);
    assert!(matches!(
        s.submit_cursor(&used_domain),
        Err(LedgerError::CursorRequest(ErrorCode::IdempotencyConflict))
    ));
    used_domain.key.id = RequestId::from_u128(1);
    let receipt = cursor_receipt(s.submit_cursor_local(&used_domain).unwrap());
    assert!(matches!(
        s.propose(&input(
            1,
            Command::NegotiateEpoch {
                epoch: RequestEpoch(1)
            }
        ))
        .unwrap(),
        Submission::Domain(DomainOutcome::Refuse {
            code: ErrorCode::IdempotencyConflict,
            ..
        })
    ));
    let mut foreign = ack(&s, 2, Position::resolved(identity(), SessionSeq(1)));
    foreign.ledger.session = SessionId::from_u128(3);
    assert!(matches!(
        s.submit_cursor(&foreign),
        Err(LedgerError::CursorRequest(ErrorCode::InvalidNamespace))
    ));
    let mut other_epoch = epoch(1);
    other_epoch.principal = ParticipantId::from_u128(2);
    s.submit_local(&other_epoch).unwrap();
    let mut thief = ack(&s, 2, Position::resolved(identity(), SessionSeq(1)));
    thief.key.principal = ParticipantId::from_u128(2);
    assert!(matches!(
        s.submit_cursor(&thief),
        Err(LedgerError::CursorRequest(ErrorCode::WrongActor))
    ));
    s.submit_local(&epoch(2)).unwrap();
    let mut floor = input(
        5,
        Command::AdvanceEpochFloor {
            minimum: RequestEpoch(2),
        },
    );
    floor.request_epoch = RequestEpoch(2);
    s.submit_local(&floor).unwrap();
    assert_eq!(
        cursor_receipt(s.submit_cursor_local(&used_domain).unwrap()),
        receipt
    );
    let unseen = ack(&s, 6, Position::resolved(identity(), s.sequence()));
    assert!(matches!(
        s.submit_cursor(&unseen),
        Err(LedgerError::CursorRequest(ErrorCode::RequestHistoryExpired))
    ));
}

#[test]
fn reserved_completion_memory_can_commit_ack_when_ordinary_admission_is_full() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
    elect(&mut s);
    s.submit_local(&epoch(1)).unwrap();
    let registration = register(&s, 1, true);
    s.submit_cursor_control_local(&registration).unwrap();
    let available =
        s.limits.memory_bytes - s.limits.completion_reserve_bytes - s.memory_stats().ordinary_used;
    let _ordinary = s
        .budget
        .reserve(BudgetKind::Payload, BudgetLane::Ordinary, available)
        .unwrap()
        .commit();
    assert!(
        s.budget
            .reserve(BudgetKind::Pending, BudgetLane::Ordinary, 1)
            .is_err()
    );
    let acknowledgment = ack(&s, 2, Position::resolved(identity(), SessionSeq(1)));
    s.submit_cursor_local(&acknowledgment).unwrap();
    assert_eq!(s.cursor_revision(), 2);
}

#[test]
fn bounded_source_certifies_only_visited_prefix_and_rejects_fabricated_ordinal() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
    elect(&mut s);
    for n in 1..=3 {
        s.submit_local(&epoch(n)).unwrap();
    }
    let (first, ids) = replay_ids(
        &s,
        Position::origin(identity()),
        ReplayLimit {
            max_items: 1,
            ..Default::default()
        },
    );
    assert_eq!(ids.len(), 1);
    assert_eq!(first, Position::after_delta(ids[0]));
    let (end, rest) = replay_ids(&s, first, ReplayLimit::default());
    assert_eq!(rest.len(), 2);
    assert_eq!(end, Position::resolved(identity(), SessionSeq(3)));
    assert!(matches!(
        s.replay(
            Position::after_delta(DeltaId {
                ordinal: 9,
                ..ids[0]
            }),
            ReplayLimit::default(),
            &mut |_| Ok(())
        ),
        Err(StreamError::Invalid(_))
    ));
    let (end, ids) = replay_ids(
        &s,
        Position::origin(identity()),
        ReplayLimit {
            max_sequences: 1,
            ..Default::default()
        },
    );
    assert_eq!(ids.len(), 1);
    assert_eq!(end, Position::resolved(identity(), SessionSeq(1)));
    assert!(matches!(
        s.replay(
            Position::origin(identity()),
            ReplayLimit {
                max_bytes: 1,
                ..Default::default()
            },
            &mut |_| panic!("over budget")
        ),
        Err(StreamError::Capacity)
    ));
}

#[test]
fn legacy_snapshot_recovers_an_explicit_history_floor() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut s =
            Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
        elect(&mut s);
        s.submit_local(&epoch(1)).unwrap();
        let envelope = SnapshotEnvelope {
            schema: 1,
            ledger: identity(),
            raft_index: s.applied_raft,
            core: s.core.encode_checkpoint().unwrap(),
        };
        let mut bytes = SNAPSHOT_MAGIC.to_vec();
        bytes.extend(postcard::to_stdvec(&envelope).unwrap());
        s.consensus.checkpoint(s.applied_raft, bytes).unwrap();
    }
    let mut s = Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
    elect(&mut s);
    assert_eq!(s.stream_bounds().floor, SessionSeq(1));
    assert!(matches!(
        s.replay(
            Position::origin(identity()),
            ReplayLimit::default(),
            &mut |_| Ok(())
        ),
        Err(StreamError::ResyncRequired(ResyncReason::HistoryExpired))
    ));
    let seed = cursor_input(
        &s,
        1,
        CursorOperation::BeginSeed {
            consumer: ConsumerId::from_u128(1),
            scope: ContentHash([1; 32]),
            filter: DeltaFilter::All,
            snapshot: s.sequence(),
            expires_at: 100,
        },
    );
    s.submit_cursor_local(&seed).unwrap();
}

fn pump_sessions(sessions: &mut [Session]) {
    for _ in 0..40 {
        let mut messages = Vec::new();
        for s in sessions.iter_mut() {
            messages.extend(s.poll().unwrap().messages);
        }
        if messages.is_empty() {
            return;
        }
        for message in messages {
            sessions[message.to as usize - 1].step(message).unwrap();
        }
    }
    panic!("transport did not quiesce");
}
/// A checkpoint is of the applied prefix, and a proposal waiting for its
/// quorum is above it (26 §3): the leader checkpoints while a domain
/// candidate waits, the candidate commits after the checkpoint, and a
/// restart from the checkpoint replays it once. A session used to refuse
/// any checkpoint while a proposal was pending, so a steady load held its
/// log past the cadence.
#[test]
fn a_checkpoint_is_taken_while_a_proposal_waits_for_its_quorum() {
    let dir = tempfile::tempdir().unwrap();
    let open = |id: u64| {
        let mut cfg = config();
        cfg.node_id = id;
        cfg.voters = vec![1, 2, 3];
        Session::open(
            dir.path().join(id.to_string()),
            identity(),
            cfg,
            SessionLimits::default(),
        )
        .unwrap()
    };
    let mut sessions = (1..=3).map(open).collect::<Vec<_>>();
    sessions[0].campaign().unwrap();
    pump_sessions(&mut sessions);
    sessions[0].propose(&epoch(1)).unwrap();
    pump_sessions(&mut sessions);
    assert_eq!(sessions[0].sequence(), SessionSeq(1));
    // The second waits: written here, its appends held back from the others.
    let second = epoch(2);
    assert!(matches!(
        sessions[0].propose(&second).unwrap(),
        Submission::Pending(_)
    ));
    let held = sessions[0].poll().unwrap().messages;
    assert_eq!(sessions[0].pending_count(), 1);
    sessions[0].checkpoint().unwrap();
    let floor = sessions[0].snapshot_index();
    assert!(floor > 0, "the applied prefix is checkpointed");
    assert_eq!(sessions[0].pending_count(), 1, "the candidate still waits");
    for message in held {
        sessions[message.to as usize - 1].step(message).unwrap();
    }
    pump_sessions(&mut sessions);
    let key = RequestKey {
        principal: second.principal,
        epoch: second.request_epoch,
        id: second.request_id,
    };
    for session in &sessions {
        assert_eq!(session.sequence(), SessionSeq(2));
    }
    let receipt = sessions[0].receipt(&key).unwrap().clone();
    assert_eq!(sessions[0].pending_count(), 0);
    // Reopened from the checkpoint, the leader replays what came after it.
    let leader = sessions.remove(0);
    drop(leader);
    let reopened = open(1);
    assert_eq!(reopened.snapshot_index(), floor);
    assert_eq!(reopened.sequence(), SessionSeq(2));
    assert_eq!(reopened.receipt(&key), Some(&receipt));
}

#[test]
fn cursor_metadata_needs_quorum_and_lost_leadership_drops_reservations() {
    let dir = tempfile::tempdir().unwrap();
    let mut sessions = (1..=3)
        .map(|id| {
            let mut cfg = config();
            cfg.node_id = id;
            cfg.voters = vec![1, 2, 3];
            Session::open(
                dir.path().join(id.to_string()),
                identity(),
                cfg,
                SessionLimits::default(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    sessions[0].campaign().unwrap();
    pump_sessions(&mut sessions);
    sessions[0].propose(&epoch(1)).unwrap();
    pump_sessions(&mut sessions);
    let registration = register(&sessions[0], 1, true);
    assert!(matches!(
        sessions[1].submit_cursor_control(&registration),
        Err(LedgerError::NotReady { .. })
    ));
    sessions[0].submit_cursor_control(&registration).unwrap();
    let isolated = sessions[0].poll().unwrap();
    assert!(isolated.cursor_committed.is_empty());
    assert_eq!(sessions[0].cursor_revision(), 0);
    for message in isolated.messages {
        sessions[message.to as usize - 1].step(message).unwrap();
    }
    pump_sessions(&mut sessions);
    for s in &sessions {
        assert_eq!(s.cursor_revision(), 1);
        assert_eq!(s.sequence(), SessionSeq(1));
    }
    let s = &mut sessions[0];
    let acknowledgment = ack(s, 2, Position::resolved(identity(), SessionSeq(1)));
    let baseline = s.memory_stats().used;
    s.submit_cursor(&acknowledgment).unwrap();
    assert!(s.memory_stats().used > baseline);
    let mut heartbeat = Message::default();
    heartbeat.set_msg_type(focal_consensus::MessageType::MsgHeartbeat);
    heartbeat.from = 2;
    heartbeat.to = 1;
    heartbeat.term = s.status().term + 1;
    s.step(heartbeat).unwrap();
    drop(s.poll().unwrap());
    assert_eq!(s.pending_count(), 0);
    assert_eq!(s.memory_stats().used, baseline);
    assert!(s.cursor_receipt(&acknowledgment.key).is_none());
    assert_eq!(s.cursor_revision(), 1);
}

#[test]
fn domain_receipt_capacity_cannot_consume_reserved_cursor_ack_slots() {
    let dir = tempfile::tempdir().unwrap();
    let mut limits = SessionLimits {
        cursor_receipts: 2,
        ..Default::default()
    };
    limits.core.max_requests = 1;
    let mut s = Session::open(dir.path(), identity(), config(), limits).unwrap();
    elect(&mut s);
    s.submit_local(&epoch(1)).unwrap();
    assert!(matches!(
        s.propose(&epoch(2)).unwrap(),
        Submission::Domain(DomainOutcome::Refuse {
            code: ErrorCode::Capacity,
            ..
        })
    ));
    let registration = register(&s, 1, true);
    s.submit_cursor_control_local(&registration).unwrap();
    let mut extra = register(&s, 2, false);
    if let CursorOperation::Register { consumer, .. } = &mut extra.command.operation {
        *consumer = ConsumerId::from_u128(2);
    }
    assert!(matches!(
        s.submit_cursor(&extra),
        Err(LedgerError::Capacity)
    ));
    let acknowledgment = ack(&s, 3, Position::resolved(identity(), SessionSeq(1)));
    let receipt = cursor_receipt(s.submit_cursor_local(&acknowledgment).unwrap());
    assert_eq!(receipt.revision, 2);
    assert_eq!(
        cursor_receipt(s.submit_cursor_local(&acknowledgment).unwrap()),
        receipt
    );
}

#[test]
fn combined_ack_and_renew_recovers_original_reply_without_reextending_lease() {
    let dir = tempfile::tempdir().unwrap();
    let request;
    let receipt;
    {
        let mut s =
            Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
        elect(&mut s);
        s.submit_local(&epoch(1)).unwrap();
        let registration = register(&s, 1, false);
        s.submit_cursor_local(&registration).unwrap();
        let token = CursorToken {
            position: Position::resolved(identity(), SessionSeq(1)),
            ..s.cursor(ConsumerId::from_u128(1)).unwrap().token
        };
        request = cursor_input(
            &s,
            2,
            CursorOperation::AcknowledgeAndRenew {
                token,
                expires_at: 2000,
            },
        );
        receipt = cursor_receipt(s.submit_cursor_local(&request).unwrap());
        s.checkpoint().unwrap();
    }
    let mut s = Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
    elect(&mut s);
    let mut retry = request;
    retry.command.expected_revision = s.cursor_revision();
    retry.command.now = 100;
    if let CursorOperation::AcknowledgeAndRenew { expires_at, .. } = &mut retry.command.operation {
        *expires_at = 2100;
    }
    assert_eq!(
        cursor_receipt(s.submit_cursor_local(&retry).unwrap()),
        receipt
    );
    assert_eq!(s.cursor(ConsumerId::from_u128(1)).unwrap().expires_at, 2000);
    let mut invalid = retry;
    invalid.key.id = RequestId::from_u128(3);
    invalid.intent_hash = ContentHash([30; 32]);
    if let CursorOperation::AcknowledgeAndRenew { token, .. } = &mut invalid.command.operation {
        token.position = Position::origin(identity());
    }
    assert!(matches!(
        s.submit_cursor(&invalid),
        Err(LedgerError::Stream(StreamError::CursorRegression))
    ));
    assert_eq!(s.cursor(ConsumerId::from_u128(1)).unwrap().expires_at, 2000);
}

#[test]
fn replicated_cursor_receipt_uses_logged_floor_across_different_cache_sizes() {
    let dir = tempfile::tempdir().unwrap();
    let mut sessions = (1..=3)
        .map(|id| {
            let mut cfg = config();
            cfg.node_id = id;
            cfg.voters = vec![1, 2, 3];
            let limits = SessionLimits {
                delta_items: if id == 1 { 2 } else { 10 },
                ..Default::default()
            };
            Session::open(dir.path().join(id.to_string()), identity(), cfg, limits).unwrap()
        })
        .collect::<Vec<_>>();
    sessions[0].campaign().unwrap();
    pump_sessions(&mut sessions);
    for n in 1..=3 {
        sessions[0].propose(&epoch(n)).unwrap();
        pump_sessions(&mut sessions);
    }
    assert_eq!(sessions[0].stream_bounds().floor, SessionSeq(1));
    assert_eq!(sessions[1].stream_bounds().floor, SessionSeq(0));
    let mut registration = register(&sessions[0], 1, true);
    if let CursorOperation::RegisterProtected { start, .. } = &mut registration.command.operation {
        *start = Position::resolved(identity(), SessionSeq(1));
    }
    sessions[0].submit_cursor_control(&registration).unwrap();
    pump_sessions(&mut sessions);
    let receipt = sessions[0].cursor_receipt(&registration.key).unwrap();
    for s in &sessions {
        assert_eq!(s.cursor_receipt(&registration.key).unwrap(), receipt);
        assert_eq!(s.stream_bounds().floor, SessionSeq(1));
    }
}

#[test]
fn due_projection_expiry_is_durable_without_growing_request_history() {
    let dir = tempfile::tempdir().unwrap();
    let limits = SessionLimits {
        delta_items: 1,
        cursor_receipts: 2,
        ..Default::default()
    };
    {
        let mut s = Session::open(dir.path(), identity(), config(), limits.clone()).unwrap();
        elect(&mut s);
        s.submit_local(&epoch(1)).unwrap();
        let registration = register(&s, 1, false);
        s.submit_cursor_local(&registration).unwrap();
        assert_eq!(s.next_cursor_expiry(), Some(1000));
        let index = s.status().committed_index;
        assert!(!s.maintain_cursor_clock_local(999).unwrap());
        assert_eq!(s.status().committed_index, index);
        assert!(matches!(
            s.propose(&epoch(2)),
            Err(LedgerError::Stream(StreamError::RetentionPinned { .. }))
        ));
        assert!(s.maintain_cursor_clock_local(1000).unwrap());
        assert_eq!(s.sequence(), SessionSeq(1));
        assert_eq!(s.cursor_revision(), 2);
        assert_eq!(s.core.snapshot().receipts.len(), 1);
        assert_eq!(s.cursor_meta.receipts.len(), 1);
        assert_eq!(s.next_cursor_expiry(), None);
        let index = s.status().committed_index;
        assert!(!s.maintain_cursor_clock_local(2000).unwrap());
        assert_eq!(s.status().committed_index, index);
        s.submit_local(&epoch(2)).unwrap();
        // Restart from log exercises the dedicated maintenance decoder.
    }
    let mut s = Session::open(dir.path(), identity(), config(), limits.clone()).unwrap();
    elect(&mut s);
    assert_eq!(s.cursor_clock(), 1000);
    assert_eq!(s.cursor_revision(), 2);
    assert_eq!(s.stream_bounds().floor, SessionSeq(1));
    assert_eq!(s.cursor_meta.receipts.len(), 1);
    s.checkpoint().unwrap();
    drop(s);
    let s = Session::open(dir.path(), identity(), config(), limits).unwrap();
    assert_eq!(s.cursor_clock(), 1000);
    assert_eq!(s.next_cursor_expiry(), None);
}

#[test]
fn internal_clock_maintenance_never_expires_protected_obligations() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
    elect(&mut s);
    s.submit_local(&epoch(1)).unwrap();
    let protected = register(&s, 1, true);
    s.submit_cursor_control_local(&protected).unwrap();
    assert_eq!(s.next_cursor_expiry(), None);
    assert!(!s.maintain_cursor_clock_local(u64::MAX).unwrap());
    let mut projection = register(&s, 2, false);
    if let CursorOperation::Register { consumer, .. } = &mut projection.command.operation {
        *consumer = ConsumerId::from_u128(2);
    }
    s.submit_cursor_local(&projection).unwrap();
    assert!(s.maintain_cursor_clock_local(u64::MAX).unwrap());
    assert_eq!(s.cursors.retention_limit(s.sequence()), SessionSeq(0));
    assert_eq!(
        s.cursor(ConsumerId::from_u128(1)).unwrap().mode,
        CursorMode::Protected
    );
}

#[test]
fn maintenance_expiry_waits_for_quorum_before_releasing_projection_pin() {
    let dir = tempfile::tempdir().unwrap();
    let mut sessions = (1..=3)
        .map(|id| {
            let mut cfg = config();
            cfg.node_id = id;
            cfg.voters = vec![1, 2, 3];
            Session::open(
                dir.path().join(id.to_string()),
                identity(),
                cfg,
                SessionLimits::default(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    sessions[0].campaign().unwrap();
    pump_sessions(&mut sessions);
    sessions[0].propose(&epoch(1)).unwrap();
    pump_sessions(&mut sessions);
    let registration = register(&sessions[0], 1, false);
    sessions[0].submit_cursor(&registration).unwrap();
    pump_sessions(&mut sessions);
    let target = sessions[0].propose_cursor_clock(1000).unwrap().unwrap();
    assert_eq!(sessions[0].cursor_clock(), 0);
    assert_eq!(
        sessions[0].cursors.retention_limit(SessionSeq(1)),
        SessionSeq(0)
    );
    let isolated = sessions[0].poll().unwrap();
    assert_eq!(sessions[0].cursor_clock(), 0);
    for message in isolated.messages {
        sessions[message.to as usize - 1].step(message).unwrap();
    }
    pump_sessions(&mut sessions);
    for s in &sessions {
        assert_eq!(s.cursor_clock(), target.clock);
        assert_eq!(s.cursor_revision(), target.revision);
        assert_eq!(s.cursors.retention_limit(SessionSeq(1)), SessionSeq(1));
        assert_eq!(s.cursor_meta.receipts.len(), 1);
    }
}

/// An expired consumer's slot returns (the audit's F62), and its name with it:
/// the owner record leaves with the row, so another principal registers the
/// name once the slot frees.
#[test]
fn an_expired_consumer_s_name_is_free_for_another_principal() {
    let dir = tempfile::tempdir().unwrap();
    let mut limits = SessionLimits::default();
    limits.cursors.max_consumers = 1;
    let mut s = Session::open(dir.path(), identity(), config(), limits).unwrap();
    elect(&mut s);
    let first = ParticipantId::from_u128(1);
    let second = ParticipantId::from_u128(2);
    s.submit_local(&epoch(1)).unwrap();
    let mut negotiate = input(
        300,
        Command::NegotiateEpoch {
            epoch: RequestEpoch(1),
        },
    );
    negotiate.principal = second;
    s.submit_local(&negotiate).unwrap();
    let one = ConsumerId::from_u128(1);
    let two = ConsumerId::from_u128(2);
    let register_as = |s: &Session, id: u128, principal: ParticipantId, consumer, now: u64| {
        let mut request = cursor_input(
            s,
            id,
            CursorOperation::Register {
                consumer,
                scope: ContentHash([8; 32]),
                filter: DeltaFilter::All,
                start: Position::origin(identity()),
                expires_at: now + 1000,
            },
        );
        request.key.principal = principal;
        request.command.now = now;
        request
    };
    cursor_receipt(
        s.submit_cursor_local(&register_as(&s, 10, first, one, 0))
            .unwrap(),
    );
    assert_eq!(s.cursor_owner(one), Some(first));
    // At the bound while the lease lives: refused.
    assert!(matches!(
        s.submit_cursor_local(&register_as(&s, 11, second, two, 500)),
        Err(LedgerError::Stream(focal_stream::StreamError::Capacity))
    ));
    assert_eq!(s.cursor_owner(one), Some(first));
    // The lease ended: the registration goes through; the expired row and
    // its owner record leave.
    cursor_receipt(
        s.submit_cursor_local(&register_as(&s, 12, second, two, 2000))
            .unwrap(),
    );
    assert!(s.cursor(one).is_none());
    assert_eq!(s.cursor_owner(one), None);
    assert_eq!(s.cursor_owner(two), Some(second));
    // The name is the other principal's to register once its slot frees.
    cursor_receipt(
        s.submit_cursor_local(&register_as(&s, 13, second, one, 4000))
            .unwrap(),
    );
    assert_eq!(s.cursor_owner(one), Some(second));
    assert!(s.cursor(two).is_none());
    assert_eq!(s.cursor_owner(two), None);
}

/// The audit's F61: a cursor command carries its receipt and its owner into
/// the metadata; it never copies the receipt map, so what a command holds
/// while it waits for its quorum is the same with two receipts as with
/// hundreds.
#[test]
fn a_cursor_command_holds_its_entry_never_the_receipts() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
    elect(&mut s);
    s.submit_local(&epoch(1)).unwrap();
    s.submit_cursor_local(&register(&s, 1, false)).unwrap();
    let position = Position::origin(identity());
    let mut held = Vec::new();
    for round in 0..2u128 {
        let before = s.memory_stats().used;
        assert!(matches!(
            s.submit_cursor(&ack(&s, 1000 + round, position)).unwrap(),
            CursorSubmission::Pending(_)
        ));
        held.push(s.memory_stats().used - before);
        assert!(s.poll().unwrap().messages.is_empty());
        for id in 0..256u128 {
            s.submit_cursor_local(&ack(&s, 2000 + round * 1000 + id, position))
                .unwrap();
        }
    }
    // Only the receipt's own encoding grows — its revision's varint, at the
    // reference multiplier — never a copy of the map.
    assert!(held[1].abs_diff(held[0]) <= 64 * 8, "{held:?}");
    assert_eq!(s.cursor_meta.receipts.len(), 1 + 2 + 512);
}

/// A poll that finds a lease past its half-life has the node renew it by
/// its own entry: no receipt, no request key; nothing before the half.
#[test]
fn a_polled_lease_past_its_half_life_is_renewed_by_the_node_s_own_entry() {
    let dir = tempfile::tempdir().unwrap();
    let consumer = ConsumerId::from_u128(1);
    {
        let mut s =
            Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
        elect(&mut s);
        s.submit_local(&epoch(1)).unwrap();
        // A lease to 1000 at clock 0.
        s.submit_cursor_local(&register(&s, 1, false)).unwrap();
        let generation = s.cursor(consumer).unwrap().token.generation;
        let revision = s.cursor_revision();
        let receipts = s.cursor_meta.receipts.len();
        let index = s.status().committed_index;
        // At 400, a renewal to 1400 would leave 600 of a term of 1000 —
        // more than half: nothing is due, nothing is written.
        assert_eq!(
            s.propose_cursor_renewal(consumer, generation, 400, 1400)
                .unwrap(),
            None
        );
        assert_eq!(s.status().committed_index, index);
        // At 500 the half has passed: 500 remain of the 1000 granted.
        let target = s
            .propose_cursor_renewal(consumer, generation, 500, 1500)
            .unwrap()
            .unwrap();
        assert_eq!(target.revision, revision + 1);
        assert!(s.poll().unwrap().messages.is_empty());
        assert_eq!(s.cursor_revision(), revision + 1);
        assert_eq!(s.cursor(consumer).unwrap().expires_at, 1500);
        assert_eq!(s.cursor_clock(), 500);
        assert_eq!(s.cursor_meta.receipts.len(), receipts);
        assert_eq!(s.next_cursor_expiry(), Some(1500));
        assert!(matches!(
            s.propose_cursor_renewal(consumer, generation + 1, 1100, 2100),
            Err(LedgerError::Stream(StreamError::WrongGeneration))
        ));
        let protected = ConsumerId::from_u128(2);
        s.submit_cursor_control_local(&cursor_input(
            &s,
            77,
            CursorOperation::RegisterProtected {
                consumer: protected,
                scope: ContentHash([8; 32]),
                filter: DeltaFilter::All,
                start: Position::origin(identity()),
            },
        ))
        .unwrap();
        let generation = s.cursor(protected).unwrap().token.generation;
        assert_eq!(
            s.propose_cursor_renewal(protected, generation, 1100, 2100)
                .unwrap(),
            None,
            "a protected consumer holds no lease to renew"
        );
    }
    // The entry replays from the log through the maintenance decoder.
    let mut s = Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
    elect(&mut s);
    assert_eq!(s.cursor(consumer).unwrap().expires_at, 1500);
    assert_eq!(s.cursor_clock(), 500);
}
