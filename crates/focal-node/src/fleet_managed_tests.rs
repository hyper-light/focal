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
            }),
            send,
            charge,
        ))
        .unwrap();
    owner.deferred_managed.back_mut().unwrap().deadline = Instant::now();
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
            let Operation::Raft { message, .. } = &frame.request.operation else {
                panic!("raft")
            };
            let mut decoded = focal_consensus::Message::default();
            decoded.merge_from_bytes(message).unwrap();
            if decoded.get_msg_type() == focal_consensus::MessageType::MsgSnapshot {
                snapshots += 1;
                if !canceled {
                    canceled = true;
                    // Cancelling a real egress send drops its sender. No caller
                    // manually reports Failure; the same owner must retry it.
                    drop(frame);
                    continue;
                }
            }
            let was_snapshot = decoded.get_msg_type() == focal_consensus::MessageType::MsgSnapshot;
            let accepted = deliver_frame(&mut learner, frame, false);
            if was_snapshot && !accepted {
                floor_rejected = true;
            }
        }
        while let Ok(frame) = replies.try_recv() {
            let Operation::Raft { message, .. } = &frame.request.operation else {
                panic!("raft")
            };
            let mut decoded = focal_consensus::Message::default();
            decoded.merge_from_bytes(message).unwrap();
            let retry_hint = decoded.get_msg_type()
                == focal_consensus::MessageType::MsgHeartbeatResponse
                || decoded.get_msg_type() == focal_consensus::MessageType::MsgAppendResponse
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
    let Operation::Raft { message, .. } = &frame.request.operation else {
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
