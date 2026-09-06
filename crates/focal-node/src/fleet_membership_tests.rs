use super::*;
use focal_consensus::{MembershipChange, NodeConfig};
use focal_ledger::SessionLimits;

#[test]
fn checked_transfer_uses_the_owner_configuration_and_preserves_domain_prefix() {
    let directory = tempfile::tempdir().unwrap();
    let ledger = LedgerId {
        tenant: TenantId::from_u128(87),
        session: SessionId::from_u128(88),
    };
    let mut sessions = (1..=3)
        .map(|node| {
            let mut config = NodeConfig::single(node, [89; 16], ledger.session.0);
            config.voters = vec![1, 2, 3];
            Session::open(
                directory.path().join(node.to_string()),
                ledger,
                config,
                SessionLimits::default(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    sessions[0].campaign().unwrap();
    for _ in 0..20 {
        let messages = sessions
            .iter_mut()
            .flat_map(|s| s.poll().unwrap().messages)
            .collect::<Vec<_>>();
        if messages.is_empty() {
            break;
        }
        for message in messages {
            sessions[message.to as usize - 1].step(message).unwrap();
        }
    }
    let session = sessions.remove(0);
    assert!(session.is_authoritative());
    let view = session.membership().unwrap();
    let budget = MemoryBudget::new(64 * 1024 * 1024, 24 * 1024 * 1024).unwrap();
    let (sender, _receiver) = mpsc::sync_channel(4);
    let (outbound, mut outgoing) = async_mpsc::channel(16);
    let (_host, mut owner) = ReplicaHost::assemble(
        session,
        ReplicaConfig::new(RootCommandId::from_u128(90)),
        ReplicaHost::wire_limits(),
        None,
        budget.clone(),
        HostSender::Direct(sender),
        outbound,
    )
    .unwrap();
    let baseline = budget.stats().used;
    for (target, index, expected, success) in [
        (
            2,
            view.configuration_index.checked_add(1).unwrap(),
            view.configuration.clone(),
            false,
        ),
        (
            2,
            view.configuration_index,
            focal_consensus::MembershipConfiguration {
                voters: vec![1, 2],
                ..Default::default()
            },
            false,
        ),
        (
            2,
            view.configuration_index,
            view.configuration.clone(),
            true,
        ),
    ] {
        let charge = budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, 192 * 1024)
            .unwrap()
            .commit();
        let (send, receive) = oneshot::channel();
        owner
            .accept(Work::Transfer(
                target,
                Some(Box::new(CheckedTransfer {
                    expected_index: index,
                    expected,
                    _charge: charge,
                })),
                send,
            ))
            .unwrap();
        let result = receive.blocking_recv().unwrap();
        if success {
            result.unwrap();
        } else {
            assert!(matches!(result, Err(LedgerError::MembershipConflict)));
        }
        assert_eq!(owner.session.sequence().0, 0);
        assert_eq!(
            owner.session.membership().unwrap().configuration_index,
            view.configuration_index
        );
        while outgoing.try_recv().is_ok() {}
        assert_eq!(budget.stats().used, baseline);
    }
}

#[test]
fn membership_reply_and_cancelled_intent_preserve_permit_lifetimes() {
    let directory = tempfile::tempdir().unwrap();
    let ledger = LedgerId {
        tenant: TenantId::from_u128(71),
        session: SessionId::from_u128(72),
    };
    let mut session = Session::open(
        directory.path(),
        ledger,
        NodeConfig::single(1, [7; 16], ledger.session.0),
        SessionLimits::default(),
    )
    .unwrap();
    session.campaign().unwrap();
    for _ in 0..4 {
        session.poll().unwrap();
    }
    let budget = MemoryBudget::new(64 * 1024 * 1024, 24 * 1024 * 1024).unwrap();
    let (sender, _receiver) = mpsc::sync_channel(4);
    let (outbound, mut outgoing) = async_mpsc::channel(4);
    let (_host, mut owner) = ReplicaHost::assemble(
        session,
        ReplicaConfig::new(RootCommandId::from_u128(73)),
        ReplicaHost::wire_limits(),
        None,
        budget.clone(),
        HostSender::Direct(sender),
        outbound,
    )
    .unwrap();
    let baseline = budget.stats().used;
    let view = owner.session.membership().unwrap();
    let request = SessionMembershipRequest {
        id: [1; 16],
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
    assert!(
        owner
            .session
            .membership_receipt(&request)
            .unwrap()
            .is_none()
    );
    let pause = owner.session.shared_wal().pause_for_test().unwrap();
    assert!(owner.session.try_poll().unwrap().is_none());
    assert!(matches!(
        owner.session.membership(),
        Err(LedgerError::Consensus(
            focal_consensus::ConsensusError::PersistencePending
        ))
    ));
    drop(pause);
    owner.drain().unwrap();
    assert!(
        owner
            .session
            .membership_receipt(&request)
            .unwrap()
            .is_some()
    );
    assert!(
        matches!(receive.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
        "durable apply alone does not release the reply"
    );
    owner.drain().unwrap();
    let reply = receive.try_recv().unwrap().unwrap();
    while outgoing.try_recv().is_ok() {}
    assert_eq!(reply.view().configuration.learners, vec![2]);
    assert!(owner.memberships.is_empty());
    assert_eq!(owner.memberships.capacity(), 0);
    assert_eq!(budget.stats().used, baseline + 192 * 1024);
    drop(reply);
    assert_eq!(budget.stats().used, baseline);

    for (id, cancel) in [(2, true), (3, false)] {
        let view = owner.session.membership().unwrap();
        let request = SessionMembershipRequest {
            id: [id; 16],
            expected_index: view.configuration_index,
            expected: view.configuration,
            change: if id == 2 {
                MembershipChange::Remove { node: 2 }
            } else {
                MembershipChange::AddLearner { node: 3 }
            },
        };
        let charge = budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, 192 * 1024)
            .unwrap()
            .commit();
        let (send, receive) = oneshot::channel();
        owner.accept_membership(
            MembershipCall {
                request: Some(request.clone()),
                response: send,
            },
            charge,
        );
        if cancel {
            drop(receive);
        } else {
            owner.memberships.front_mut().unwrap().deadline = Instant::now();
            owner.expire_pending();
            assert!(matches!(
                receive.blocking_recv().unwrap(),
                Err(LedgerError::OutcomeUnknown)
            ));
        }
        owner.expire_pending();
        assert_eq!(owner.memberships.capacity(), 0);
        assert_eq!(budget.stats().used, baseline);
        // Cancellation/timeouts release only the waiter. Accepted Raft intent
        // remains live and is available for exact retry once durable.
        assert!(
            owner
                .session
                .membership_receipt(&request)
                .unwrap()
                .is_none()
        );
        for _ in 0..3 {
            owner.drain().unwrap();
        }
        while outgoing.try_recv().is_ok() {}
        assert!(
            owner
                .session
                .membership_receipt(&request)
                .unwrap()
                .is_some()
        );
    }
}

#[test]
fn stop_with_uncommitted_membership_preserves_unknown_intent_without_checkpoint() {
    let directory = tempfile::tempdir().unwrap();
    let ledger = LedgerId {
        tenant: TenantId::from_u128(81),
        session: SessionId::from_u128(82),
    };
    let mut sessions = (1..=3)
        .map(|id| {
            let mut config = NodeConfig::single(id, [8; 16], ledger.session.0);
            config.voters = vec![1, 2, 3];
            Session::open(
                directory.path().join(id.to_string()),
                ledger,
                config,
                SessionLimits::default(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    sessions[0].campaign().unwrap();
    for _ in 0..20 {
        let messages = sessions
            .iter_mut()
            .flat_map(|session| session.poll().unwrap().messages)
            .collect::<Vec<_>>();
        if messages.is_empty() {
            break;
        }
        for message in messages {
            sessions[message.to as usize - 1].step(message).unwrap();
        }
    }
    let session = sessions.remove(0);
    assert!(session.is_authoritative());
    let view = session.membership().unwrap();
    let request = SessionMembershipRequest {
        id: [4; 16],
        expected_index: view.configuration_index,
        expected: view.configuration,
        change: MembershipChange::AddLearner { node: 4 },
    };
    let budget = MemoryBudget::new(64 * 1024 * 1024, 24 * 1024 * 1024).unwrap();
    let (sender, _receiver) = mpsc::sync_channel(4);
    let (outbound, _outgoing) = async_mpsc::channel(16);
    let (_host, mut owner) = ReplicaHost::assemble(
        session,
        ReplicaConfig::new(RootCommandId::from_u128(83)),
        ReplicaHost::wire_limits(),
        None,
        budget.clone(),
        HostSender::Direct(sender),
        outbound,
    )
    .unwrap();
    let charge = budget
        .reserve(BudgetKind::Control, BudgetLane::Completion, 192 * 1024)
        .unwrap()
        .commit();
    let (send, receive) = oneshot::channel();
    owner.accept_membership(
        MembershipCall {
            request: Some(request.clone()),
            response: send,
        },
        charge,
    );
    owner.drain().unwrap(); // Persist locally; deliberately deliver no peer traffic.
    assert_eq!(owner.session.pending_count(), 1);
    assert!(
        owner
            .session
            .membership_receipt(&request)
            .unwrap()
            .is_none()
    );
    let (stop_send, stop_receive) = oneshot::channel();
    assert!(owner.accept(Work::Stop(stop_send)).unwrap());
    stop_receive.blocking_recv().unwrap().unwrap();
    owner.close();
    assert!(matches!(
        receive.blocking_recv().unwrap(),
        Err(LedgerError::OutcomeUnknown)
    ));
    drop(owner);
    let mut config = NodeConfig::single(1, [8; 16], ledger.session.0);
    config.voters = vec![1, 2, 3];
    let reopened = Session::open(
        directory.path().join("1"),
        ledger,
        config,
        SessionLimits::default(),
    )
    .unwrap();
    assert!(reopened.membership_receipt(&request).unwrap().is_none());
    assert!(!reopened.membership().unwrap().configuration.contains(4));
}
