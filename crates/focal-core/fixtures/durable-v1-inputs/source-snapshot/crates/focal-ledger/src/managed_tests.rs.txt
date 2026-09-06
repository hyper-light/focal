fn durable_managed_support(session: &mut Session) {
    session.begin_managed_support().unwrap();
    assert!(
        session.managed_support().is_err()
            || session.consensus.decoder_floor_ready(managed_format_hash())
    );
    session.poll().unwrap();
    assert!(session.managed_support().is_ok());
}
fn stream_control(id: u128, command: RequestStreamCommand) -> RequestStreamControlInput {
    RequestStreamControlInput {
        cluster: [9; 16],
        ledger: identity(),
        principal: ParticipantId::from_u128(1),
        id: RequestId::from_u128(id),
        command,
    }
}
fn stream_register(s: &mut Session, slot: u32, window: u32) -> RequestStreamIdentity {
    durable_managed_support(s);
    let request = stream_control(
        1000 + u128::from(slot),
        RequestStreamCommand::Register {
            slot,
            expected_generation: 0,
            owner: RequestId::from_u128(2000 + u128::from(slot)),
            window,
        },
    );
    assert!(matches!(
        s.propose_request_stream(&request).unwrap(),
        RequestStreamSubmission::Pending(_)
    ));
    assert!(s.request_stream_receipt(&request).unwrap().is_none());
    s.poll().unwrap();
    let receipt = s.request_stream_receipt(&request).unwrap().unwrap();
    assert!(receipt.raft_index > 0);
    match receipt.outcome {
        RequestStreamControlOutcome::Registered(RequestStreamState::Active { stream, .. }) => {
            stream
        }
        _ => panic!("unexpected registration"),
    }
}
fn managed_claim(stream: RequestStreamIdentity, ordinal: u64) -> ManagedAuthenticatedInput {
    let id = ClaimId::from_u128(100 + u128::from(ordinal) + u128::from(stream.slot) * 100);
    let validation = NewValidation {
        id: ValidationId::from_u128(10000 + u128::from(ordinal) + u128::from(stream.slot) * 100),
        content: ValidationContent {
            ledger: identity(),
            schema: 1,
            claim: id,
            kind: ValidationKind::Receipt,
            phase: ValidationPhase::WholeWork,
            mode: ValidationMode::Required,
            description: "receipt".into(),
            quality_bar: None,
            evaluator: stream.principal,
            handlers: Vec::new(),
            evidence_schemas: Default::default(),
            contributed_by: [stream.principal].into_iter().collect(),
            policy_revision: 1,
        },
    };
    let claim = NewClaim {
        id,
        content: ClaimContent {
            ledger: identity(),
            schema: 1,
            occurrence: OccurrenceId(id.0),
            description: "managed work".into(),
            relations: [
                Relation {
                    kind: RelationKind::Issuer,
                    target: RelationTarget::Participant(stream.principal),
                },
                Relation {
                    kind: RelationKind::Subject,
                    target: RelationTarget::Participant(ParticipantId::from_u128(2)),
                },
                Relation {
                    kind: RelationKind::ClaimAction,
                    target: RelationTarget::Action(ActionType::Work),
                },
                Relation {
                    kind: RelationKind::CausedBy,
                    target: RelationTarget::Root(RootCommandId::from_u128(1)),
                },
            ]
            .into_iter()
            .collect(),
            scopes: Default::default(),
            requirements: vec![RequirementRef {
                id: validation.id,
                specification: validation.content.specification_hash().unwrap(),
            }],
            deadline: None,
        },
        validations: vec![validation],
    };
    let old = input(10 + u128::from(ordinal), Command::GenerateClaim { claim });
    ManagedAuthenticatedInput {
        key: ManagedRequestKey {
            stream,
            ordinal,
            id: old.request_id,
        },
        expected_revision: None,
        authority: old.authority,
        command: old.command,
    }
}
fn commit_managed(s: &mut Session, input: &ManagedAuthenticatedInput) -> ManagedReceipt {
    let submitted = s.propose_managed(input).unwrap();
    assert!(
        matches!(submitted, ManagedSubmission::Pending(_)),
        "{submitted:?}"
    );
    let events = s.poll().unwrap();
    assert_eq!(events.managed_committed.len(), 1);
    assert!(events.committed.is_empty());
    s.managed_receipt(
        &input.key,
        managed_command_hash(input).unwrap(),
        ManagedRequestFamily::Domain,
    )
    .unwrap()
    .unwrap()
    .clone()
}
#[test]
fn managed_domain_cursor_ack_control_revision_and_checkpoint_tail_are_exact() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
    elect(&mut s);
    let stream = stream_register(&mut s, 0, 2);
    let domain = managed_claim(stream, 1);
    let first = commit_managed(&mut s, &domain);
    assert_eq!(s.sequence(), SessionSeq(1));
    assert_eq!(s.graph_sequence(), s.sequence());
    s.audit_graph().unwrap();
    assert!(s.core.snapshot().epochs.is_empty());
    assert!(s.core.snapshot().receipts.is_empty());
    let old = register(&s, 33, false);
    let cursor = ManagedCursorInput {
        key: ManagedRequestKey {
            stream,
            ordinal: 2,
            id: RequestId::from_u128(33),
        },
        intent_hash: old.intent_hash,
        command: old.command,
    };
    assert!(matches!(
        s.propose_managed_cursor(&cursor, false).unwrap(),
        ManagedSubmission::Pending(_)
    ));
    s.poll().unwrap();
    let second = s
        .managed_receipt(
            &cursor.key,
            cursor.intent_hash,
            ManagedRequestFamily::Cursor,
        )
        .unwrap()
        .unwrap()
        .clone();
    assert_eq!(second.sequence, first.sequence);
    assert!(second.raft_index > first.raft_index);
    assert!(s.cursor_meta.receipts.is_empty());
    assert!(matches!(
        s.request_streams.state(stream.principal, 0).unwrap(),
        RequestStreamState::Active { revision: 1, .. }
    ));
    let third = managed_claim(stream, 3);
    assert!(matches!(
        s.propose_managed(&third),
        Err(LedgerError::Managed(ManagedError::Capacity))
    ));
    let ack = stream_control(
        3000,
        RequestStreamCommand::Acknowledge {
            stream,
            expected_revision: 1,
            through: 2,
            receipts: vec![
                ManagedReceiptAck {
                    key: first.key,
                    receipt_hash: first.content_hash().unwrap(),
                },
                ManagedReceiptAck {
                    key: second.key,
                    receipt_hash: second.content_hash().unwrap(),
                },
            ],
        },
    );
    // Exhaust ordinary admission. Reclamation still has its reserved lane and
    // never allocates an ordinary receipt slot.
    let stats = s.memory_stats();
    let pressure = s
        .budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Ordinary,
            stats.limit - stats.completion_reserve - stats.ordinary_used,
        )
        .unwrap()
        .commit();
    assert!(matches!(
        s.propose_request_stream(&ack).unwrap(),
        RequestStreamSubmission::Pending(_)
    ));
    assert!(
        s.managed_receipt(&first.key, first.intent_hash, ManagedRequestFamily::Domain)
            .unwrap()
            .is_some()
    );
    s.poll().unwrap();
    drop(pressure);
    assert!(matches!(
        s.managed_receipt(&first.key, first.intent_hash, ManagedRequestFamily::Domain),
        Err(LedgerError::Managed(ManagedError::Retired { through: 2 }))
    ));
    assert!(s.cursor(ConsumerId::from_u128(1)).is_some()); // ACK never releases consumer retention.
    let original = s.request_stream_receipt(&ack).unwrap().unwrap().clone();
    s.checkpoint().unwrap();
    let tail = commit_managed(&mut s, &third);
    drop(s);
    let mut recovered =
        Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
    elect(&mut recovered);
    recovered.audit_graph().unwrap();
    assert_eq!(recovered.sequence(), SessionSeq(2));
    assert_eq!(
        recovered.request_stream_receipt(&ack).unwrap(),
        Some(&original)
    );
    assert_eq!(
        recovered
            .managed_receipt(&third.key, tail.intent_hash, ManagedRequestFamily::Domain)
            .unwrap(),
        Some(&tail)
    );
    assert!(
        matches!(recovered.propose_request_stream(&ack).unwrap(),RequestStreamSubmission::Committed(r) if *r==original)
    );
}
#[test]
fn managed_seals_and_closed_generations_fence_delayed_domain_and_cursor_work() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
    elect(&mut s);
    let stream = stream_register(&mut s, 0, 2);
    let work = managed_claim(stream, 1);
    let hash = managed_command_hash(&work).unwrap();
    let seal = stream_control(
        4000,
        RequestStreamCommand::Seal {
            key: work.key,
            expected_revision: 1,
            family: ManagedRequestFamily::Domain,
            intent_hash: hash,
        },
    );
    let submitted = s.propose_managed(&work).unwrap();
    assert!(
        matches!(submitted, ManagedSubmission::Pending(_)),
        "{submitted:?}"
    );
    assert!(matches!(
        s.propose_request_stream(&seal),
        Err(LedgerError::Capacity)
    ));
    s.poll().unwrap();
    s.propose_request_stream(&seal).unwrap();
    s.poll().unwrap();
    assert!(
        matches!(&s.request_stream_receipt(&seal).unwrap().unwrap().outcome,RequestStreamControlOutcome::Sealed(r) if matches!(r.outcome,ManagedReceiptOutcome::Domain(_)))
    );
    // A seal that discovers a committed outcome cannot erase it on close.
    let close = stream_control(
        4001,
        RequestStreamCommand::Close {
            stream,
            expected_revision: 2,
            issued_through: 1,
        },
    );
    assert!(matches!(
        s.propose_request_stream(&close),
        Err(LedgerError::Managed(ManagedError::Conflict))
    ));
    let receipt = s
        .managed_receipt(&work.key, hash, ManagedRequestFamily::Domain)
        .unwrap()
        .unwrap()
        .clone();
    let ack = stream_control(
        4002,
        RequestStreamCommand::Acknowledge {
            stream,
            expected_revision: 2,
            through: 1,
            receipts: vec![ManagedReceiptAck {
                key: work.key,
                receipt_hash: receipt.content_hash().unwrap(),
            }],
        },
    );
    s.propose_request_stream(&ack).unwrap();
    s.poll().unwrap();
    let gap = ManagedRequestKey {
        stream,
        ordinal: 2,
        id: RequestId::from_u128(999),
    };
    let seal = stream_control(
        4003,
        RequestStreamCommand::Seal {
            key: gap,
            expected_revision: 3,
            family: ManagedRequestFamily::Cursor,
            intent_hash: ContentHash([0; 32]),
        },
    );
    s.propose_request_stream(&seal).unwrap();
    s.poll().unwrap();
    let close = stream_control(
        4004,
        RequestStreamCommand::Close {
            stream,
            expected_revision: 4,
            issued_through: 2,
        },
    );
    s.propose_request_stream(&close).unwrap();
    s.poll().unwrap();
    s.checkpoint().unwrap();
    drop(s);
    let mut s = Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
    elect(&mut s);
    assert!(matches!(
        s.propose_managed(&work),
        Err(LedgerError::Managed(ManagedError::Closed { generation: 1 }))
    ));
    let future = ManagedRequestKey {
        stream: RequestStreamIdentity {
            generation: 2,
            ..stream
        },
        ..gap
    };
    let read = s
        .request_stream_read(
            stream.principal,
            &RequestStreamQuery::Receipt { key: future },
        )
        .unwrap()
        .to_owned()
        .unwrap();
    assert!(matches!(
        read.result,
        RequestStreamReadResult::Receipt {
            resolution: ManagedReceiptResolution::Unknown,
            ..
        }
    ));
    let again = stream_control(
        4005,
        RequestStreamCommand::Register {
            slot: 0,
            expected_generation: 1,
            owner: RequestId::from_u128(45),
            window: 2,
        },
    );
    s.propose_request_stream(&again).unwrap();
    s.poll().unwrap();
    assert!(matches!(
        s.request_streams.state(stream.principal, 0).unwrap(),
        RequestStreamState::Active {
            stream: RequestStreamIdentity { generation: 2, .. },
            ..
        }
    ));
    assert!(matches!(
        s.propose_request_stream(&close).unwrap_err(),
        LedgerError::Managed(ManagedError::Conflict)
    ));
}
fn managed_pump(sessions: &mut [Session], partition: bool) {
    for _ in 0..64 {
        let mut messages = Vec::new();
        for session in sessions.iter_mut() {
            messages.extend(session.poll().unwrap().messages);
        }
        let mut delivered = false;
        for message in messages {
            if partition && (message.from == 1 || message.to == 1) {
                continue;
            }
            let to = message.to as usize - 1;
            if let Some(session) = sessions.get_mut(to) {
                session.step(message).unwrap();
                delivered = true;
            }
        }
        if !delivered {
            return;
        }
    }
    panic!("managed simulation did not quiesce");
}
#[test]
fn managed_all_voter_support_and_partitioned_seal_survive_leader_change_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let configuration = |node| {
        let mut cfg = config();
        cfg.node_id = node;
        cfg.voters = vec![1, 2, 3];
        cfg
    };
    let mut sessions = (1..=3)
        .map(|id| {
            Session::open(
                dir.path().join(id.to_string()),
                identity(),
                configuration(id),
                SessionLimits::default(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    sessions[0].campaign().unwrap();
    managed_pump(&mut sessions, false);
    for session in &mut sessions {
        durable_managed_support(session);
    }
    let registration = stream_control(
        9000,
        RequestStreamCommand::Register {
            slot: 0,
            expected_generation: 0,
            owner: RequestId::from_u128(99),
            window: 2,
        },
    );
    assert!(matches!(
        sessions[0].propose_request_stream(&registration),
        Err(LedgerError::Managed(ManagedError::Unsupported))
    ));
    let facts = sessions
        .iter()
        .map(|s| s.managed_support().unwrap())
        .collect::<Vec<_>>();
    let mut wrong = facts[1].clone();
    wrong.cluster = [8; 16];
    assert!(sessions[0].record_managed_support(2, wrong).is_err());
    assert!(
        sessions[0]
            .record_managed_support(3, facts[1].clone())
            .is_err()
    );
    sessions[0]
        .record_managed_support(2, facts[1].clone())
        .unwrap();
    assert!(matches!(
        sessions[0].propose_request_stream(&registration),
        Err(LedgerError::Managed(ManagedError::Unsupported))
    ));
    for session in &mut sessions {
        for fact in &facts {
            session
                .record_managed_support(fact.node, fact.clone())
                .unwrap();
        }
    }
    sessions[0].propose_request_stream(&registration).unwrap();
    managed_pump(&mut sessions, false);
    let stream = match sessions[0]
        .request_streams
        .state(registration.principal, 0)
        .unwrap()
    {
        RequestStreamState::Active { stream, .. } => stream,
        _ => panic!("missing stream"),
    };
    let request = managed_claim(stream, 1);
    let hash = managed_command_hash(&request).unwrap();
    sessions[0].propose_managed(&request).unwrap();
    managed_pump(&mut sessions, true);
    assert!(
        sessions[0]
            .managed_receipt(&request.key, hash, ManagedRequestFamily::Domain)
            .unwrap()
            .is_none()
    );
    let mut next = None;
    for _ in 0..80 {
        sessions[1].tick().unwrap();
        sessions[2].tick().unwrap();
        managed_pump(&mut sessions, true);
        next = (1..3).find(|i| sessions[*i].is_authoritative());
        if next.is_some() {
            break;
        }
    }
    let leader = next.expect("majority elects authoritative successor");
    let seal = stream_control(
        9001,
        RequestStreamCommand::Seal {
            key: request.key,
            expected_revision: 1,
            family: ManagedRequestFamily::Domain,
            intent_hash: hash,
        },
    );
    sessions[leader].propose_request_stream(&seal).unwrap();
    managed_pump(&mut sessions, true);
    let original = sessions[leader]
        .request_stream_receipt(&seal)
        .unwrap()
        .unwrap()
        .clone();
    assert!(
        matches!(&original.outcome,RequestStreamControlOutcome::Sealed(receipt) if matches!(receipt.outcome,ManagedReceiptOutcome::Sealed{..}))
    );
    managed_pump(&mut sessions, false);
    // The leader retransmits after the partition heals; no stale prepared domain
    // candidate can cross the already committed ordinal fence.
    for _ in 0..4 {
        sessions[leader].tick().unwrap();
        managed_pump(&mut sessions, false);
    }
    for session in &mut sessions {
        assert_eq!(session.sequence(), SessionSeq(0));
        assert_eq!(
            session.request_stream_receipt(&seal).unwrap(),
            Some(&original)
        );
        assert!(
            matches!(session.propose_managed(&request).unwrap(),ManagedSubmission::Committed(receipt) if matches!(receipt.outcome,ManagedReceiptOutcome::Sealed{..}))
        );
        session.checkpoint().unwrap();
    }
    drop(sessions);
    let mut recovered = Vec::new();
    for node in 1..=3 {
        let session = Session::open(
            dir.path().join(node.to_string()),
            identity(),
            configuration(node),
            SessionLimits::default(),
        )
        .unwrap();
        assert_eq!(
            session.request_stream_receipt(&seal).unwrap(),
            Some(&original)
        );
        assert_eq!(session.sequence(), SessionSeq(0));
        assert!(session.managed_support.nodes.is_empty());
        recovered.push(session);
    }
    // A committed activation plus each local durable floor survives a leader
    // restart. Node 1 remains unreachable: no fresh support response is available
    // from it, but nodes 2/3 are still a quorum for new managed work.
    let mut next = None;
    for _ in 0..80 {
        recovered[1].tick().unwrap();
        recovered[2].tick().unwrap();
        managed_pump(&mut recovered, true);
        next = (1..3).find(|i| recovered[*i].is_authoritative());
        if next.is_some() {
            break;
        }
    }
    let leader = next.expect("restarted quorum elects a leader");
    assert!(recovered[leader].managed_protocol_active());
    assert!(recovered[leader].needs_managed_support(1));
    assert_eq!(
        recovered[leader]
            .request_stream_receipt_parts(seal.cluster, seal.principal, seal.id, &seal.command)
            .unwrap(),
        Some(&original)
    );
    let next = managed_claim(stream, 2);
    assert!(matches!(
        recovered[leader].propose_managed(&next).unwrap(),
        ManagedSubmission::Pending(_)
    ));
    managed_pump(&mut recovered, true);
    for replica in &recovered[1..] {
        assert_eq!(replica.sequence(), SessionSeq(1));
        assert!(
            replica
                .managed_receipt(
                    &next.key,
                    managed_command_hash(&next).unwrap(),
                    ManagedRequestFamily::Domain
                )
                .unwrap()
                .is_some()
        );
        assert!(replica.managed_support.nodes.is_empty());
    }
}
#[test]
fn managed_activation_requires_real_joiner_decoder_and_configuration_refresh() {
    let dir = tempfile::tempdir().unwrap();
    let mut leader = Session::open(
        dir.path().join("one"),
        identity(),
        config(),
        SessionLimits::default(),
    )
    .unwrap();
    elect(&mut leader);
    stream_register(&mut leader, 0, 1);
    let mut joining = Session::open(
        dir.path().join("two"),
        identity(),
        NodeConfig::joining(2, [9; 16], identity().session.0, vec![1], vec![]),
        SessionLimits::default(),
    )
    .unwrap();
    durable_managed_support(&mut joining);
    let support = joining.managed_support().unwrap();
    assert_eq!(support.voters, vec![1]);
    assert_eq!(support.configuration_index, 0);
    let change = membership_request(&leader, 70, MembershipChange::AddLearner { node: 2 });
    assert!(matches!(
        leader.propose_membership(&change),
        Err(LedgerError::Managed(ManagedError::Unsupported))
    ));
    assert!(leader.record_managed_support(3, support.clone()).is_err());
    let mut stale = support.clone();
    stale.voters = vec![3];
    assert!(leader.record_managed_support(2, stale).is_err());
    leader.record_managed_support(2, support.clone()).unwrap();
    leader.propose_membership(&change).unwrap();
    leader.poll().unwrap();
    assert!(leader.managed_support.nodes.is_empty());
    assert!(leader.record_managed_support(2, support).is_err()); // it is now a member, so bootstrap stale fact no longer suffices.
    let promote = membership_request(&leader, 71, MembershipChange::Promote { node: 2 });
    assert!(matches!(
        leader.propose_membership(&promote),
        Err(LedgerError::Managed(ManagedError::Unsupported))
    ));
}

// Deliver through the actual authenticated parser. A rejected snapshot is
// transport failure, not ingress acceptance; Raft must receive that feedback to
// leave its paused snapshot state and retry after the receiver fsyncs its floor.
fn pump_learner_floor(
    sessions: &mut [Session],
    snapshot_attempts: &mut usize,
    rejected: &mut usize,
) {
    use focal_consensus::{PbMessageExt, SnapshotStatus};
    for _ in 0..64 {
        let messages = sessions
            .iter_mut()
            .flat_map(|s| s.poll().unwrap().messages)
            .collect::<Vec<_>>();
        if messages.is_empty() {
            return;
        }
        for message in messages {
            let snapshot = !message.get_snapshot().is_empty();
            if snapshot {
                *snapshot_attempts += 1;
            }
            let from = message.from;
            let to = message.to;
            let term = message.term;
            let index = message.get_snapshot().get_metadata().index;
            let encoded = message.write_to_bytes().unwrap();
            let receiver = &mut sessions[to as usize - 1];
            if receiver.consensus.required_decoder().is_none() && !receiver.persistence_pending() {
                let demanded = receiver.managed_support_demanded();
                assert!(matches!(
                    receiver.step_authenticated(from + 10, &encoded),
                    Err(LedgerError::Consensus(ConsensusError::MalformedMessage(_)))
                ));
                assert_eq!(receiver.consensus.required_decoder(), None);
                assert_eq!(receiver.managed_support_demanded(), demanded);
                assert!(!receiver.persistence_pending());
            }
            let result = sessions[to as usize - 1].step_authenticated(from, &encoded);
            let status = match result {
                Ok(()) => SnapshotStatus::Finish,
                Err(LedgerError::Consensus(ConsensusError::PersistencePending)) => {
                    *rejected += 1;
                    assert!(!sessions[to as usize - 1].managed_protocol_active());
                    assert_eq!(sessions[to as usize - 1].sequence(), SessionSeq(0));
                    SnapshotStatus::Failure
                }
                Err(error) => panic!("unexpected learner error: {error:?}"),
            };
            if snapshot {
                sessions[from as usize - 1]
                    .report_snapshot_at(to, term, index, status)
                    .unwrap();
            }
        }
    }
    panic!("learner messages did not quiesce");
}

#[test]
fn existing_learner_fsyncs_floor_before_first_managed_append() {
    managed_existing_learner_recovery(false);
}

#[test]
fn existing_learner_retries_rejected_v5_snapshot_after_floor_fsync() {
    managed_existing_learner_recovery(true);
}

fn managed_existing_learner_recovery(checkpoint: bool) {
    let dir = tempfile::tempdir().unwrap();
    let configuration = |node| {
        let mut cfg = config();
        cfg.node_id = node;
        cfg.learners = vec![2];
        cfg
    };
    let mut sessions = (1..=2)
        .map(|node| {
            Session::open(
                dir.path().join(node.to_string()),
                identity(),
                configuration(node),
                SessionLimits::default(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    sessions[0].campaign().unwrap();
    let mut snapshot_attempts = 0;
    let mut rejected = 0;
    pump_learner_floor(&mut sessions, &mut snapshot_attempts, &mut rejected);
    assert!(sessions[0].is_authoritative());
    assert_eq!(sessions[1].consensus.required_decoder(), None);
    let stream = stream_register(&mut sessions[0], 0, 1);
    let request = managed_claim(stream, 1);
    sessions[0].propose_managed(&request).unwrap();
    sessions[0].poll().unwrap(); // learner is disconnected while these commit
    if checkpoint {
        sessions[0].checkpoint().unwrap();
    }
    assert_eq!(sessions[1].consensus.required_decoder(), None);
    for _ in 0..40 {
        sessions[0].tick().unwrap();
        pump_learner_floor(&mut sessions, &mut snapshot_attempts, &mut rejected);
        if sessions[1].sequence() == SessionSeq(1) {
            break;
        }
    }
    assert!(
        rejected > 0,
        "first managed history is deferred until local floor durability"
    );
    assert_eq!(sessions[1].sequence(), SessionSeq(1));
    assert!(sessions[1].managed_protocol_active());
    assert!(sessions[1].managed_support().is_ok());
    assert!(sessions[1].managed_support.nodes.is_empty());
    assert_eq!(
        sessions[0]
            .managed_receipt(
                &request.key,
                managed_command_hash(&request).unwrap(),
                ManagedRequestFamily::Domain
            )
            .unwrap(),
        sessions[1]
            .managed_receipt(
                &request.key,
                managed_command_hash(&request).unwrap(),
                ManagedRequestFamily::Domain
            )
            .unwrap()
    );
    if checkpoint {
        assert!(
            snapshot_attempts >= 2,
            "rejected snapshot must be retransmitted"
        );
    } else {
        assert_eq!(snapshot_attempts, 0);
    }
    drop(sessions);
    let recovered = Session::open(
        dir.path().join("2"),
        identity(),
        configuration(2),
        SessionLimits::default(),
    )
    .unwrap();
    assert_eq!(recovered.sequence(), SessionSeq(1));
    assert!(recovered.managed_support().is_ok());
}
#[test]
fn managed_committed_follower_cursor_and_domain_rebuild_use_completion_capacity() {
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
    managed_pump(&mut sessions, false);
    for session in &mut sessions {
        durable_managed_support(session);
    }
    for index in 1..3 {
        let fact = sessions[index].managed_support().unwrap();
        sessions[0].record_managed_support(fact.node, fact).unwrap();
    }
    let request = stream_control(
        7000,
        RequestStreamCommand::Register {
            slot: 0,
            expected_generation: 0,
            owner: RequestId::from_u128(7),
            window: 2,
        },
    );
    sessions[0].propose_request_stream(&request).unwrap();
    managed_pump(&mut sessions, false);
    let stream = match sessions[0]
        .request_streams
        .state(request.principal, 0)
        .unwrap()
    {
        RequestStreamState::Active { stream, .. } => stream,
        _ => panic!("registration"),
    };
    let pressure = sessions
        .iter()
        .skip(1)
        .map(|s| {
            let stats = s.memory_stats();
            s.budget
                .reserve(
                    BudgetKind::Payload,
                    BudgetLane::Ordinary,
                    stats.limit - stats.completion_reserve - stats.ordinary_used,
                )
                .unwrap()
                .commit()
        })
        .collect::<Vec<_>>();
    let claim = managed_claim(stream, 1);
    sessions[0].propose_managed(&claim).unwrap();
    managed_pump(&mut sessions, false);
    let old = register(&sessions[0], 7001, false);
    let cursor = ManagedCursorInput {
        key: ManagedRequestKey {
            stream,
            ordinal: 2,
            id: RequestId::from_u128(7001),
        },
        intent_hash: old.intent_hash,
        command: old.command,
    };
    sessions[0].propose_managed_cursor(&cursor, false).unwrap();
    managed_pump(&mut sessions, false);
    for session in &sessions {
        assert_eq!(session.sequence(), SessionSeq(1));
        session.audit_graph().unwrap();
        assert!(session.cursor(ConsumerId::from_u128(1)).is_some());
        assert!(
            session
                .managed_receipt(
                    &cursor.key,
                    cursor.intent_hash,
                    ManagedRequestFamily::Cursor
                )
                .unwrap()
                .is_some()
        );
    }
    drop(pressure);
}
#[test]
fn managed_slots_are_independent_and_ack_manifest_conflicts_do_not_publish() {
    let dir = tempfile::tempdir().unwrap();
    let limits = SessionLimits {
        request_streams: RequestStreamLimits {
            max_slots: 2,
            max_window: 1,
            ..RequestStreamLimits::default()
        },
        ..SessionLimits::default()
    };
    let mut s = Session::open(dir.path(), identity(), config(), limits).unwrap();
    elect(&mut s);
    let one = stream_register(&mut s, 0, 1);
    let two = stream_register(&mut s, 1, 1);
    let work = managed_claim(one, 1);
    let receipt = commit_managed(&mut s, &work);
    assert!(matches!(
        s.propose_managed(&managed_claim(one, 2)),
        Err(LedgerError::Managed(ManagedError::Capacity))
    ));
    let other = managed_claim(two, 1);
    commit_managed(&mut s, &other);
    let mut wrong = receipt.content_hash().unwrap();
    wrong.0[0] ^= 1;
    let ack = stream_control(
        8000,
        RequestStreamCommand::Acknowledge {
            stream: one,
            expected_revision: 1,
            through: 1,
            receipts: vec![ManagedReceiptAck {
                key: work.key,
                receipt_hash: wrong,
            }],
        },
    );
    let before = s.memory_stats().used;
    assert!(matches!(
        s.propose_request_stream(&ack),
        Err(LedgerError::Managed(ManagedError::Conflict))
    ));
    assert_eq!(s.memory_stats().used, before);
    assert_eq!(s.pending_count(), 0);
    assert_eq!(
        s.managed_receipt(&work.key, receipt.intent_hash, ManagedRequestFamily::Domain)
            .unwrap(),
        Some(&receipt)
    );
    let third = stream_control(
        8001,
        RequestStreamCommand::Register {
            slot: 2,
            expected_generation: 0,
            owner: RequestId::from_u128(8002),
            window: 1,
        },
    );
    assert!(matches!(
        s.propose_request_stream(&third),
        Err(LedgerError::Managed(ManagedError::Capacity))
    ));
    assert_eq!(s.sequence(), SessionSeq(2));
}
#[test]
fn managed_bounded_hash_and_independent_audit_reservation_fail_before_admission() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
    elect(&mut s);
    let stream = stream_register(&mut s, 0, 1);
    let original = managed_claim(stream, 1);
    let mut oversized = original.clone();
    if let Command::GenerateClaim { claim } = &mut oversized.command {
        claim.content.description = "x".repeat(s.limits.core.max_command_bytes + 1);
    }
    let baseline = s.memory_stats().used;
    assert!(matches!(
        s.propose_managed(&oversized),
        Err(LedgerError::Capacity)
    ));
    assert_eq!(s.memory_stats().used, baseline);
    assert_eq!(s.pending_count(), 0);
    let stats = s.memory_stats();
    let available = stats.limit - stats.completion_reserve - stats.ordinary_used;
    let pressure = s
        .budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Ordinary,
            available - s.staging_bytes() - 128 * 1024,
        )
        .unwrap()
        .commit();
    let baseline = s.memory_stats().used;
    assert!(matches!(
        s.propose_managed(&original),
        Err(LedgerError::Memory(MemoryError::Capacity { .. }))
    ));
    assert_eq!(s.memory_stats().used, baseline);
    assert_eq!(s.pending_count(), 0);
    assert_eq!(s.sequence(), SessionSeq(0));
    drop(pressure);
    commit_managed(&mut s, &original);
}
#[test]
fn managed_exhausted_generation_can_close_but_never_wraps_on_register() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
    elect(&mut s);
    let original = stream_register(&mut s, 0, 1);
    // A representable historical snapshot at the final generation avoids an
    // infeasible 2^64-1-cycle setup. All encoded state/receipt scopes agree.
    let mut checkpoint = s.request_streams.checkpoint();
    let data = &mut checkpoint.slots[0];
    let final_stream = RequestStreamIdentity {
        generation: u64::MAX,
        ..original
    };
    if let RequestStreamState::Active { stream, .. } = &mut data.state {
        *stream = final_stream;
    }
    data.latest.as_mut().unwrap().outcome = RequestStreamControlOutcome::Registered(data.state);
    s.request_streams
        .restore(checkpoint, s.sequence(), s.applied_raft, &s.budget)
        .unwrap();
    s.checkpoint().unwrap();
    let close = stream_control(
        9100,
        RequestStreamCommand::Close {
            stream: final_stream,
            expected_revision: 1,
            issued_through: 0,
        },
    );
    s.propose_request_stream(&close).unwrap();
    s.poll().unwrap();
    let register = stream_control(
        9101,
        RequestStreamCommand::Register {
            slot: 0,
            expected_generation: u64::MAX,
            owner: RequestId::from_u128(91),
            window: 1,
        },
    );
    assert!(matches!(
        s.propose_request_stream(&register),
        Err(LedgerError::Managed(ManagedError::Capacity))
    ));
    s.checkpoint().unwrap();
    drop(s);
    let s = Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
    assert_eq!(
        s.request_streams.state(original.principal, 0).unwrap(),
        RequestStreamState::Vacant {
            slot: 0,
            generation: u64::MAX
        }
    );
}
#[test]
fn managed_capability_is_lazy_durable_and_recovered_decoder_must_confirm_before_raft() {
    let dir = tempfile::tempdir().unwrap();
    let mut session =
        Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
    elect(&mut session);
    assert!(!session.managed_support_demanded());
    assert!(session.consensus.required_decoder().is_none());
    assert!(session.managed_support().is_err());
    assert!(!session.managed_support_demanded());
    session.submit_local(&epoch(1)).unwrap();
    session.checkpoint().unwrap();
    assert!(session.consensus.required_decoder().is_none());
    let index = session.applied_raft;
    let sequence = session.sequence();
    session.begin_managed_support().unwrap();
    assert!(session.managed_support_demanded());
    assert!(session.consensus.required_decoder().is_none());
    assert!(session.managed_support().is_err());
    session.poll().unwrap();
    assert_eq!(session.sequence(), sequence);
    assert_eq!(session.applied_raft, index);
    let support = session.managed_support().unwrap();
    session.checkpoint().unwrap();
    drop(session);
    let mut raw = DurableNode::open(config(), dir.path()).unwrap();
    assert_eq!(raw.required_decoder(), Some(support.format_hash.0));
    assert!(matches!(
        raw.campaign(),
        Err(ConsensusError::DecoderUnconfirmed)
    ));
    assert!(matches!(
        raw.confirm_decoder([7; 32]),
        Err(ConsensusError::DecoderMismatch)
    ));
    let recovered = Session::from_node(identity(), raw, SessionLimits::default()).unwrap();
    assert_eq!(recovered.sequence(), sequence);
    assert!(recovered.managed_support_demanded());
    assert_eq!(
        recovered.managed_support().unwrap().format_hash,
        support.format_hash
    );
}
