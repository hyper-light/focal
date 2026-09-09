fn placement_request(
    session: &Session,
    kind: SessionFenceKind,
    operation: u128,
) -> SessionPlacementRequest {
    let members: BTreeMap<_, _> = session
        .membership()
        .unwrap()
        .configuration
        .voters
        .into_iter()
        .map(|id| (id, 1))
        .collect();
    let route = if kind == SessionFenceKind::Created {
        1
    } else {
        2
    };
    SessionPlacementRequest {
        expected_index: session.placement().map_or(0, |f| f.index.0),
        expected_configuration_index: session.membership().unwrap().configuration_index,
        operation: OperationId::from_u128(operation),
        kind,
        from_route: RouteEpoch(route - 1),
        to_route: RouteEpoch(route),
        membership_epoch: 1,
        placement_epoch: route,
        placement: PlacementSpec {
            policy: focal_directory::PlacementPolicy {
                durability: focal_directory::DurabilityIntent {
                    survive: focal_directory::FailureClass::Node,
                    max_failures: 0,
                },
                residency: Default::default(),
                home_regions: Default::default(),
                required_memory: 0,
            },
            placement: focal_directory::Placement {
                preferred_leader: *members.keys().next().unwrap(),
                voters: members.clone(),
                materializers: members.clone(),
                content_copies: members,
            },
        },
    }
}
#[test]
fn placement_lifecycle_keeps_domain_clock_and_replays_exact_empty_prefix_fences() {
    // A cutover carries the group's actual membership epoch, which several
    // committed learner changes may have raised above the one this placement
    // implies; both the live rule and the checkpoint validator accept it.
    for (checkpoint, cutover_epoch) in [(false, 1), (true, 1), (true, 3)] {
        let dir = tempfile::tempdir().unwrap();
        let mut session =
            Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
        elect(&mut session);
        let created = placement_request(&session, SessionFenceKind::Created, 1);
        session.propose_placement(&created).unwrap();
        assert_eq!(session.pending_count(), 1);
        assert!(session.placement_witness(&created).unwrap().is_none());
        session.poll().unwrap();
        let first = session.placement_receipt(&created).unwrap().unwrap();
        assert_eq!(first.sequence, SessionSeq(0));
        assert!(first.index.0 > 0);
        let mut stale = placement_request(&session, SessionFenceKind::Cutover, 2);
        stale.membership_epoch = 0;
        assert!(matches!(
            session.propose_placement(&stale),
            Err(LedgerError::PlacementConflict)
        ));
        let mut cutover = placement_request(&session, SessionFenceKind::Cutover, 2);
        cutover.membership_epoch = cutover_epoch;
        session.propose_placement(&cutover).unwrap();
        session.poll().unwrap();
        let sealed = session.placement_receipt(&cutover).unwrap().unwrap();
        assert_eq!(sealed.membership_epoch, cutover_epoch);
        assert_eq!(sealed.sequence, SessionSeq(0));
        assert!(sealed.index > first.index);
        let input = input(
            77,
            Command::NegotiateEpoch {
                epoch: RequestEpoch(1),
            },
        );
        assert!(matches!(
            session.propose(&input),
            Err(LedgerError::Capacity)
        ));
        let membership = membership_request(&session, 99, MembershipChange::AddLearner { node: 5 });
        assert!(matches!(
            session.propose_membership(&membership),
            Err(LedgerError::Capacity)
        ));
        if checkpoint {
            session.checkpoint().unwrap();
        }
        drop(session);
        let mut session =
            Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
        elect(&mut session);
        assert_eq!(
            session.placement_receipt(&cutover).unwrap(),
            Some(sealed.clone())
        );
        let mut activated = placement_request(&session, SessionFenceKind::Activated, 2);
        activated.membership_epoch = cutover_epoch;
        session.propose_placement(&activated).unwrap();
        session.poll().unwrap();
        let final_fence = session.placement_receipt(&activated).unwrap().unwrap();
        assert_eq!(final_fence.sequence, SessionSeq(0));
        assert!(final_fence.index > sealed.index);
        assert_eq!(session.active_route(), Some(RouteEpoch(2)));
        assert_eq!(session.graph_sequence(), SessionSeq(0));
        session.submit_local(&input).unwrap();
        assert_eq!(session.sequence(), SessionSeq(1));
        if checkpoint {
            session.checkpoint().unwrap();
        }
        drop(session);
        let mut session =
            Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
        elect(&mut session);
        assert_eq!(
            session.placement_receipt(&activated).unwrap(),
            Some(final_fence)
        );
        assert_eq!(session.placement_receipt(&cutover).unwrap(), Some(sealed));
        let witness = session.placement_witness(&activated).unwrap().unwrap();
        assert_eq!(witness.node(), 1);
        assert_eq!(witness.genesis(), session.placement_genesis().unwrap());
        let mut conflicting = activated.clone();
        conflicting.placement.policy.required_memory = 1;
        assert!(matches!(
            session.propose_placement(&conflicting),
            Err(LedgerError::PlacementConflict)
        ));
    }
}
#[test]
fn placement_record_needs_quorum_and_same_proof_recovers_on_every_voter() {
    let dir = tempfile::tempdir().unwrap();
    let mut sessions: Vec<_> = (1..=3)
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
        .collect();
    sessions[0].campaign().unwrap();
    pump_sessions(&mut sessions);
    pump_sessions(&mut sessions);
    let request = placement_request(&sessions[0], SessionFenceKind::Created, 1);
    sessions[0].propose_placement(&request).unwrap();
    let messages = sessions[0].poll().unwrap().messages;
    assert!(sessions[0].placement_witness(&request).unwrap().is_none());
    for message in messages {
        sessions[message.to as usize - 1].step(message).unwrap();
    }
    pump_sessions(&mut sessions);
    let first = sessions[0].placement_receipt(&request).unwrap().unwrap();
    for session in &sessions {
        assert_eq!(
            session.placement_receipt(&request).unwrap(),
            Some(first.clone())
        );
        assert_eq!(
            session.placement_witness(&request).unwrap().unwrap().node(),
            session.status().node_id
        );
    }
    let genesis = sessions[0].placement_genesis().unwrap();
    drop(sessions);
    for id in 1..=3 {
        let mut cfg = config();
        cfg.node_id = id;
        cfg.voters = vec![1, 2, 3];
        let session = Session::open(
            dir.path().join(id.to_string()),
            identity(),
            cfg,
            SessionLimits::default(),
        )
        .unwrap();
        assert_eq!(
            session.placement_receipt(&request).unwrap(),
            Some(first.clone())
        );
        assert_eq!(session.placement_genesis().unwrap(), genesis);
    }
}
#[test]
fn placement_admission_uses_completion_reserve_and_snapshot_rejects_forged_fence() {
    let dir = tempfile::tempdir().unwrap();
    let mut session =
        Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
    elect(&mut session);
    let stats = session.budget.stats();
    let ordinary = stats.limit - stats.completion_reserve - stats.ordinary_used;
    let pressure = session
        .budget
        .reserve(BudgetKind::Payload, BudgetLane::Ordinary, ordinary)
        .unwrap()
        .commit();
    let request = placement_request(&session, SessionFenceKind::Created, 1);
    session.propose_placement(&request).unwrap();
    session.poll().unwrap();
    assert!(session.placement_witness(&request).unwrap().is_some());
    drop(pressure);
    let mut malformed = PlacementState {
        active: session.placement_state.active.clone(),
        cutover: None,
    };
    malformed.active.as_mut().unwrap().fence.record_hash.0[0] ^= 1;
    assert!(
        session
            .validate_placement_snapshot(
                &malformed,
                session.applied_raft,
                session.status().term,
                session.sequence()
            )
            .is_err()
    );
}

#[test]
fn placement_unknown_outcome_loses_local_reservation_and_retries_after_real_leader_loss() {
    let dir = tempfile::tempdir().unwrap();
    let mut sessions: Vec<_> = (1..=3)
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
        .collect();
    sessions[0].campaign().unwrap();
    pump_sessions(&mut sessions);
    pump_sessions(&mut sessions);
    let request = placement_request(&sessions[0], SessionFenceKind::Created, 1);
    sessions[0].propose_placement(&request).unwrap();
    drop(sessions[0].poll().unwrap());
    assert_eq!(sessions[0].pending_count(), 1);
    assert!(sessions[0].placement_witness(&request).unwrap().is_none());
    for _ in 0..60 {
        for s in sessions.iter_mut().skip(1) {
            s.tick().unwrap();
        }
        for _ in 0..10 {
            let mut messages = Vec::new();
            for s in sessions.iter_mut().skip(1) {
                messages.extend(s.poll().unwrap().messages);
            }
            if messages.is_empty() {
                break;
            }
            for message in messages {
                if message.to != 1 {
                    sessions[message.to as usize - 1].step(message).unwrap();
                }
            }
        }
        if sessions.iter().skip(1).any(Session::is_authoritative) {
            break;
        }
    }
    let leader = sessions
        .iter()
        .position(|s| s.status().node_id != 1 && s.is_authoritative())
        .unwrap();
    sessions[leader].propose_placement(&request).unwrap();
    for _ in 0..10 {
        for s in sessions.iter_mut().skip(1) {
            s.tick().unwrap();
        }
        pump_sessions(&mut sessions);
    }
    assert_eq!(sessions[0].pending_count(), 0);
    assert!(!sessions[0].is_authoritative());
    let receipt = sessions[leader]
        .placement_receipt(&request)
        .unwrap()
        .unwrap();
    for s in &sessions {
        assert_eq!(
            s.placement_receipt(&request).unwrap(),
            Some(receipt.clone())
        );
        assert!(s.placement_witness(&request).unwrap().is_some());
    }
}
