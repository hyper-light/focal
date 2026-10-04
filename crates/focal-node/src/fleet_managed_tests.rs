use super::*;
use focal_consensus::{MembershipChange, NodeConfig};
use focal_ledger::SessionLimits;
fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(811),
        session: SessionId::from_u128(812),
    }
}
fn actor() -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId::from_u128(813),
        tenants: [ledger().tenant].into_iter().collect(),
        role: PeerRole::Actor,
    })
    .unwrap()
}
fn register() -> RequestEnvelope {
    RequestEnvelope {
        protocol: MANAGED_PROTOCOL_VERSION,
        ledger: ledger(),
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(1),
        operation: Operation::RequestStreamControl {
            cluster: [81; 16],
            command: RequestStreamCommand::Register {
                slot: 0,
                expected_generation: 0,
                owner: RequestId([1; 16]),
                window: 4,
            },
        },
    }
}
fn assemble(session: Session) -> (Owner, async_mpsc::Receiver<ReplicationFrame>, MemoryBudget) {
    let budget = MemoryBudget::new(64 * 1024 * 1024, 24 * 1024 * 1024).unwrap();
    let (sender, _receiver) = mpsc::sync_channel(4);
    let (outbound, outgoing) = async_mpsc::channel(32);
    let (_, mut owner) = ReplicaHost::assemble(
        session,
        ReplicaConfig::new(RootCommandId::from_u128(814)),
        ReplicaHost::wire_limits(),
        None,
        budget.clone(),
        HostSender::Direct(sender),
        outbound,
    )
    .unwrap();
    owner.nonblocking = true;
    (owner, outgoing, budget)
}
fn session(path: &std::path::Path, node: u64) -> Session {
    let config = if node == 1 {
        NodeConfig::single(1, [81; 16], ledger().session.0)
    } else {
        NodeConfig::joining(node, [81; 16], ledger().session.0, vec![1], vec![])
    };
    let mut session = Session::open(path, ledger(), config, SessionLimits::default()).unwrap();
    if node == 1 {
        session.campaign().unwrap();
        for _ in 0..4 {
            session.poll().unwrap();
        }
    }
    session
}
fn support(owner: &mut Owner) -> Result<ManagedSupportReply, LedgerError> {
    let charge = owner
        .budget
        .reserve(BudgetKind::Control, BudgetLane::Completion, 192 * 1024)
        .unwrap()
        .commit();
    let (send, receive) = oneshot::channel();
    owner.accept_managed_support(
        SupportCall {
            received: None,
            response: send,
        },
        charge,
    );
    receive.blocking_recv().unwrap()
}
fn settle(owner: &mut Owner) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while owner.session.persistence_pending() || owner.session.has_ready() {
        owner.progress_group().unwrap();
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(1));
    }
    owner.progress_managed().unwrap();
}
#[test]
fn passive_support_is_readonly_and_canceled_floor_input_never_registers() {
    let directory = tempfile::tempdir().unwrap();
    let (owner, outgoing, budget) = assemble(session(directory.path(), 1));
    let mut owner = owner;
    assert!(matches!(
        support(&mut owner),
        Err(LedgerError::Managed(
            focal_ledger::ManagedError::Unsupported
        ))
    ));
    assert!(!owner.session.managed_support_demanded());
    let baseline = budget.stats().used;
    let pause = owner.session.shared_wal().pause_for_test().unwrap();
    let verified = verify_request(actor(), register(), &owner.client_limits).unwrap();
    let charge = budget
        .reserve(BudgetKind::Pending, BudgetLane::Ordinary, 128 * 1024)
        .unwrap()
        .commit();
    let (send, mut receive) = oneshot::channel();
    owner
        .accept(Work::Request(
            Box::new(AdmittedRequest {
                verified,
                witness: None,
                native: None,
            }),
            send,
            charge,
        ))
        .unwrap();
    assert!(owner.session.persistence_pending());
    assert!(owner.session.managed_support_demanded());
    assert_eq!(owner.deferred_managed.len(), 1);
    assert!(matches!(
        receive.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        owner.session.managed_support(),
        Err(LedgerError::Consensus(
            focal_consensus::ConsensusError::PersistencePending
        ))
    ));
    drop(receive);
    owner.progress_managed().unwrap();
    assert!(owner.session.persistence_pending());
    assert!(owner.deferred_managed.is_empty());
    assert!(owner.deferred_backing.is_none());
    assert_eq!(budget.stats().used, baseline);
    // Deadline expiry releases deferred input even while the same disk fence
    // is still paused. It does not create a registration or retract a write.
    let mut request = register();
    request.request_id = RequestId::from_u128(2);
    let verified = verify_request(actor(), request, &owner.client_limits).unwrap();
    let charge = budget
        .reserve(BudgetKind::Pending, BudgetLane::Ordinary, 128 * 1024)
        .unwrap()
        .commit();
    let (send, mut expired) = oneshot::channel();
    owner
        .accept(Work::Request(
            Box::new(AdmittedRequest {
                verified,
                witness: None,
                native: None,
            }),
            send,
            charge,
        ))
        .unwrap();
    owner.deferred_managed.back_mut().unwrap().deadline = 0;
    owner.progress_managed().unwrap();
    assert!(owner.session.persistence_pending());
    assert!(owner.deferred_managed.is_empty());
    let response = expired.try_recv().unwrap();
    assert!(matches!(
        response.envelope().result,
        Response::Error(AccessError::OutcomeUnknown)
    ));
    drop(response);
    assert!(owner.deferred_backing.is_none());
    assert_eq!(budget.stats().used, baseline);
    pause.resume().unwrap();
    settle(&mut owner);
    assert!(owner.deferred_managed.is_empty());
    assert_eq!(owner.session.sequence(), SessionSeq(0));
    let page = owner
        .session
        .request_stream_read(actor().principal(), &RequestStreamQuery::Slot { slot: 0 })
        .unwrap()
        .to_owned()
        .unwrap();
    assert!(matches!(
        page.result,
        RequestStreamReadResult::Slot(RequestStreamState::Vacant { generation: 0, .. })
    ));
    assert_eq!(budget.stats().used, baseline);
    owner.close();
    drop(owner);
    drop(outgoing);
    assert_eq!(budget.stats().used, 0);
}
/// A node the directory names for the session that the log does not hold
/// yet — a healing placement's replacement copy, admitted by the agent on
/// every hosted copy before the log names it — is asked for its promise
/// before any admission of it is queued: a native group admits a learner
/// only once its leader holds the promise, and the node cannot push it,
/// not being a member (the drained leader's heal, 2026-10-02). A learner
/// is asked too: its promotion wants its promise at the configuration
/// that admitted it.
#[test]
fn discovery_asks_the_nodes_the_directory_names_before_the_log_does() {
    let directory = tempfile::tempdir().unwrap();
    let mut initial = session(&directory.path().join("one"), 1);
    initial.begin_managed_support().unwrap();
    initial.poll().unwrap();
    let (mut owner, outgoing, budget) = assemble(initial);
    // Alone, nothing to ask: the only voter is this node.
    let alone = support(&mut owner).unwrap();
    assert_eq!(alone.targets().count(), 0);
    drop(alone);
    // Named by the directory, not by the log: asked.
    owner.admitted = vec![2];
    let named = support(&mut owner).unwrap();
    assert_eq!(named.targets().collect::<Vec<_>>(), [2]);
    drop(named);
    // Once its promise is held at the current configuration, no longer.
    let mut joined = session(&directory.path().join("two"), 2);
    joined.begin_managed_support().unwrap();
    joined.poll().unwrap();
    let fact = joined.managed_support().unwrap();
    owner.session.record_managed_support(2, fact).unwrap();
    let held = support(&mut owner).unwrap();
    assert_eq!(held.targets().count(), 0);
    drop(held);
    owner.close();
    drop(owner);
    drop(outgoing);
    drop(joined);
    assert_eq!(budget.stats().used, 0);
}
#[test]
fn trusted_membership_nominates_real_joining_decoder_and_waits_for_fact() {
    let directory = tempfile::tempdir().unwrap();
    let mut initial = session(&directory.path().join("one"), 1);
    initial.begin_managed_support().unwrap();
    initial.poll().unwrap();
    let input = verify_request(actor(), register(), &WireLimits::default())
        .unwrap()
        .into_request_stream_control()
        .unwrap();
    initial.propose_request_stream(&input).unwrap();
    initial.poll().unwrap();
    let (mut owner, mut outgoing, budget) = assemble(initial);
    let baseline = budget.stats().used;
    let view = owner.session.membership().unwrap();
    let request = SessionMembershipRequest {
        id: [5; 16],
        expected_index: view.configuration_index,
        expected: view.configuration,
        change: MembershipChange::AddLearner { node: 2 },
    };
    let charge = budget
        .reserve(BudgetKind::Control, BudgetLane::Completion, 192 * 1024)
        .unwrap()
        .commit();
    let (send, mut receive) = oneshot::channel();
    owner.accept_membership(
        MembershipCall {
            request: Some(request.clone()),
            response: send,
        },
        charge,
    );
    assert_eq!(owner.memberships.len(), 1);
    assert!(!owner.memberships[0].proposed);
    assert!(
        owner
            .session
            .membership_receipt(&request)
            .unwrap()
            .is_none()
    );
    let demanded = support(&mut owner).unwrap();
    assert_eq!(demanded.targets().collect::<Vec<_>>(), [2]);
    drop(demanded);
    let mut joined = session(&directory.path().join("two"), 2);
    assert!(!joined.status().voters.contains(&2));
    joined.begin_managed_support().unwrap();
    joined.poll().unwrap();
    let fact = joined.managed_support().unwrap();
    assert_eq!(fact.configuration_index, 0);
    assert_eq!(fact.voters, [1]);
    owner.session.record_managed_support(2, fact).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        owner.drain().unwrap();
        while outgoing.try_recv().is_ok() {}
        match receive.try_recv() {
            Ok(Ok(reply)) => {
                assert_eq!(reply.view().configuration.learners, [2]);
                drop(reply);
                break;
            }
            Ok(Err(error)) => panic!("{error:?}"),
            Err(oneshot::error::TryRecvError::Empty) => {}
            Err(error) => panic!("{error:?}"),
        };
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(owner.memberships.is_empty());
    assert_eq!(budget.stats().used, baseline);
    owner.close();
    drop(owner);
    drop(outgoing);
    drop(joined);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn snapshot_owner_retries_admission_drop_cancellation_and_unprepared_learner() {
    let directory = tempfile::tempdir().unwrap();
    let config = |node| {
        let mut config = NodeConfig::single(node, [81; 16], ledger().session.0);
        config.voters = vec![1];
        config.learners = vec![2];
        config
    };
    let mut initial = Session::open(
        directory.path().join("leader"),
        ledger(),
        config(1),
        SessionLimits::default(),
    )
    .unwrap();
    initial.campaign().unwrap();
    for _ in 0..4 {
        initial.poll().unwrap();
    }
    initial.begin_managed_support().unwrap();
    initial.poll().unwrap();
    let input = verify_request(actor(), register(), &WireLimits::default())
        .unwrap()
        .into_request_stream_control()
        .unwrap();
    initial.propose_request_stream(&input).unwrap();
    initial.poll().unwrap();
    let receipt = initial
        .request_stream_receipt(&input)
        .unwrap()
        .unwrap()
        .clone();
    initial.checkpoint().unwrap();
    let learner = Session::open(
        directory.path().join("learner"),
        ledger(),
        config(2),
        SessionLimits::default(),
    )
    .unwrap();
    assert!(!learner.managed_support_demanded());
    let (mut leader, mut outgoing, leader_budget) = assemble(initial);
    let (mut learner, mut replies, learner_budget) = assemble(learner);
    let mut admission_failed = false;
    let mut canceled = false;
    let mut floor_rejected = false;
    let mut snapshots = 0;
    for _ in 0..80 {
        settle(&mut leader);
        leader.tick().unwrap();
        settle(&mut leader);
        while let Ok(frame) = outgoing.try_recv() {
            let (Operation::Raft { message, .. } | Operation::RaftOrdered { message, .. }) =
                &frame.request.operation
            else {
                panic!("raft")
            };
            let mut decoded = focal_consensus::Message::default();
            decoded.merge_from_bytes(message).unwrap();
            if decoded.msg_type == focal_consensus::MessageType::MsgSnapshot as i32 {
                snapshots += 1;
                if !canceled {
                    canceled = true;
                    // Cancelling a real egress send drops its sender. No caller
                    // manually reports Failure; the same owner must retry it.
                    drop(frame);
                    continue;
                }
            }
            let was_snapshot = decoded.msg_type == focal_consensus::MessageType::MsgSnapshot as i32;
            let accepted = deliver_frame(&mut learner, frame, false);
            if was_snapshot && !accepted {
                floor_rejected = true;
            }
        }
        while let Ok(frame) = replies.try_recv() {
            let (Operation::Raft { message, .. } | Operation::RaftOrdered { message, .. }) =
                &frame.request.operation
            else {
                panic!("raft")
            };
            let mut decoded = focal_consensus::Message::default();
            decoded.merge_from_bytes(message).unwrap();
            let retry_hint = decoded.msg_type
                == focal_consensus::MessageType::MsgHeartbeatResponse as i32
                || decoded.msg_type == focal_consensus::MessageType::MsgAppendResponse as i32
                    && decoded.reject;
            let force_admission = retry_hint && !admission_failed;
            let dropped = leader.dropped_snapshots;
            deliver_frame(&mut leader, frame, force_admission);
            if force_admission {
                admission_failed = true;
                assert_eq!(leader.dropped_snapshots, dropped + 1);
            }
        }
        settle(&mut learner);
        settle(&mut leader);
        if learner
            .session
            .request_stream_receipt(&input)
            .unwrap()
            .is_some()
        {
            break;
        }
    }
    assert!(
        admission_failed && canceled && floor_rejected,
        "admission={admission_failed} canceled={canceled} floor={floor_rejected} snapshots={snapshots} leader={:?} learner={:?}",
        leader.session.status(),
        learner.session.status()
    );
    assert!(snapshots >= 3);
    assert_eq!(
        learner.session.request_stream_receipt(&input).unwrap(),
        Some(&receipt)
    );
    assert!(learner.session.managed_support().is_ok());
    leader.close();
    learner.close();
    drop(leader);
    drop(learner);
    drop(outgoing);
    drop(replies);
    assert_eq!(leader_budget.stats().used, 0);
    assert_eq!(learner_budget.stats().used, 0);
}
fn deliver_frame(owner: &mut Owner, mut frame: ReplicationFrame, force_admission: bool) -> bool {
    settle(owner);
    let (Operation::Raft { message, .. } | Operation::RaftOrdered { message, .. }) =
        &frame.request.operation
    else {
        panic!("raft")
    };
    let mut decoded = focal_consensus::Message::default();
    decoded.merge_from_bytes(message).unwrap();
    let peer = AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId::from_u128(u128::from(decoded.from) + 1000),
        tenants: [ledger().tenant].into_iter().collect(),
        role: PeerRole::Node {
            node_id: decoded.from,
        },
    })
    .unwrap();
    let verified = verify_request(peer, frame.request.clone(), &owner.limits).unwrap();
    let charge = owner
        .budget
        .reserve(BudgetKind::Pending, BudgetLane::Completion, 256 * 1024)
        .unwrap()
        .commit();
    let (send, mut receive) = oneshot::channel();
    let max_frame = owner.limits.max_frame_bytes;
    if force_admission {
        owner.limits.max_frame_bytes = 1;
    }
    owner
        .accept(Work::Request(
            Box::new(AdmittedRequest {
                verified,
                witness: None,
                native: None,
            }),
            send,
            charge,
        ))
        .unwrap();
    // Keep the transport admission restriction through async Ready completion.
    settle(owner);
    owner.limits.max_frame_bytes = max_frame;
    let reply = receive.try_recv().unwrap();
    let accepted = matches!(reply.envelope().result, Response::PeerAccepted);
    frame.report_snapshot(accepted);
    accepted
}

/// A learner added after the log was compacted is seeded whoever made the
/// change: Raft discards a snapshot that does not name its recipient, so a
/// replica whose stored snapshot is older than the configuration it has
/// applied checkpoints at its period. The owner that resolved the addition
/// alone used to; once leadership had moved off it, the leader that
/// followed sent the learner a snapshot that did not name it at every
/// probe, and the learner was never seeded (the drained leader's heal,
/// macOS CI at 27b0531). The change here is the log's own, as a leader
/// before this owner made it, and no request of it ever reaches the owner.
#[test]
fn a_learner_added_past_the_snapshot_is_seeded_whoever_made_the_change() {
    let directory = tempfile::tempdir().unwrap();
    let mut initial = session(&directory.path().join("leader"), 1);
    initial.checkpoint().unwrap();
    let floor = initial.snapshot_index();
    assert!(floor > 0, "the log is compacted");
    let view = initial.membership().unwrap();
    initial
        .propose_membership(&SessionMembershipRequest {
            id: [9; 16],
            expected_index: view.configuration_index,
            expected: view.configuration,
            change: MembershipChange::AddLearner { node: 2 },
        })
        .unwrap();
    for _ in 0..4 {
        initial.poll().unwrap();
    }
    let added = initial.configuration_index();
    assert!(added > floor, "the learner is added past the snapshot");
    let learner = session(&directory.path().join("learner"), 2);
    let (mut leader, mut outgoing, leader_budget) = assemble(initial);
    let (mut learner, mut replies, learner_budget) = assemble(learner);
    for _ in 0..80 {
        settle(&mut leader);
        leader.tick().unwrap();
        settle(&mut leader);
        while let Ok(frame) = outgoing.try_recv() {
            deliver_frame(&mut learner, frame, false);
        }
        while let Ok(frame) = replies.try_recv() {
            deliver_frame(&mut leader, frame, false);
        }
        settle(&mut learner);
        if learner.session.scalars().applied_index >= added {
            break;
        }
    }
    assert!(
        leader.session.snapshot_index() >= added,
        "the leader's snapshot names the learner: {} below {added}",
        leader.session.snapshot_index()
    );
    assert!(
        learner.session.scalars().applied_index >= added,
        "the learner was never seeded: {:?}",
        learner.session.status()
    );
    // A change that adds no one asks for no checkpoint: the snapshot that
    // names the learner names every member left once it is removed.
    let refreshed = leader.session.snapshot_index();
    let view = leader.session.membership().unwrap();
    leader
        .session
        .propose_membership(&SessionMembershipRequest {
            id: [10; 16],
            expected_index: view.configuration_index,
            expected: view.configuration,
            change: MembershipChange::Remove { node: 2 },
        })
        .unwrap();
    for _ in 0..4 {
        settle(&mut leader);
        leader.tick().unwrap();
        while outgoing.try_recv().is_ok() {}
    }
    assert!(leader.session.configuration_index() > refreshed);
    assert_eq!(leader.session.snapshot_index(), refreshed);
    leader.close();
    learner.close();
    drop(leader);
    drop(learner);
    drop(outgoing);
    drop(replies);
    assert_eq!(leader_budget.stats().used, 0);
    assert_eq!(learner_budget.stats().used, 0);
}

/// A peer the driver could not reach is told to the core (27 §3.3) — but a
/// core fenced by a write it still persists refuses the report, which is
/// then told next period, the report keeping its place on the owner's own
/// channel; the session is not stopped for a hint about a peer.
#[test]
fn a_lost_peer_reported_while_a_write_persists_is_told_next_period() {
    let directory = tempfile::tempdir().unwrap();
    let (mut owner, _outgoing, budget) = assemble(session(directory.path(), 1));
    let pause = owner.session.shared_wal().pause_for_test().unwrap();
    let verified = verify_request(actor(), register(), &owner.client_limits).unwrap();
    let charge = budget
        .reserve(BudgetKind::Pending, BudgetLane::Ordinary, 128 * 1024)
        .unwrap()
        .commit();
    let (send, _receive) = oneshot::channel();
    owner
        .accept(Work::Request(
            Box::new(AdmittedRequest {
                verified,
                witness: None,
                native: None,
            }),
            send,
            charge,
        ))
        .unwrap();
    assert!(owner.session.persistence_pending());
    owner.lost_sender.try_send(7).unwrap();
    owner.report_lost().unwrap();
    assert_eq!(owner.unreachable, 0, "the fenced core was not told");
    assert!(owner.session.persistence_pending());
    drop(pause);
    settle(&mut owner);
    owner.report_lost().unwrap();
    assert_eq!(owner.unreachable, 1, "told once the write was durable");
    assert!(owner.lost.try_recv().is_err(), "the report was consumed");
}

/// Reports of lost exchanges are held for the core each peer once: a peer
/// reported again while it is held is coalesced, and peers beyond the bound
/// on those held are dropped — both counted, neither a reason to grow —
/// and every held peer is told once the core can be told.
#[test]
fn lost_peers_are_held_each_once_and_told_when_the_core_can_be() {
    let directory = tempfile::tempdir().unwrap();
    let (mut owner, _outgoing, budget) = assemble(session(directory.path(), 1));
    let pause = owner.session.shared_wal().pause_for_test().unwrap();
    let verified = verify_request(actor(), register(), &owner.client_limits).unwrap();
    let charge = budget
        .reserve(BudgetKind::Pending, BudgetLane::Ordinary, 128 * 1024)
        .unwrap()
        .commit();
    let (send, _receive) = oneshot::channel();
    owner
        .accept(Work::Request(
            Box::new(AdmittedRequest {
                verified,
                witness: None,
                native: None,
            }),
            send,
            charge,
        ))
        .unwrap();
    assert!(owner.session.persistence_pending());
    // As many distinct peers as the bound, one of them reported three times.
    for peer in 1..=u64::try_from(LOST_PEERS).unwrap() {
        owner.lost_sender.try_send(peer).unwrap();
    }
    owner.report_lost().unwrap();
    assert_eq!(owner.lost_peers.len(), LOST_PEERS);
    assert_eq!(
        (owner.unreachable, owner.lost_coalesced, owner.lost_dropped),
        (0, 0, 0)
    );
    owner.lost_sender.try_send(7).unwrap();
    owner.lost_sender.try_send(7).unwrap();
    owner.lost_sender.try_send(5_000).unwrap();
    owner.report_lost().unwrap();
    assert_eq!(
        owner.lost_peers.len(),
        LOST_PEERS,
        "the set never grows past the bound"
    );
    assert_eq!(
        (owner.unreachable, owner.lost_coalesced, owner.lost_dropped),
        (0, 2, 1),
        "the held peer coalesced twice, the peer beyond the bound dropped"
    );
    drop(pause);
    settle(&mut owner);
    owner.report_lost().unwrap();
    assert!(owner.lost_peers.is_empty(), "every held peer was told");
    assert_eq!(owner.unreachable, u64::try_from(LOST_PEERS).unwrap());
}
