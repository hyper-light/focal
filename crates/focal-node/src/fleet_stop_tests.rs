use super::*;
use focal_consensus::{ConsensusError, NodeConfig};
use focal_ledger::SessionLimits;

fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(910),
        session: SessionId::from_u128(911),
    }
}
fn config() -> NodeConfig {
    NodeConfig::single(1, [91; 16], ledger().session.0)
}

#[test]
fn stop_drains_election_readiness_without_waiting_and_reopens_the_same_prefix() {
    for before_stop in [0, 1] {
        let directory = tempfile::tempdir().unwrap();
        let mut session = Session::open(
            directory.path(),
            ledger(),
            config(),
            SessionLimits::default(),
        )
        .unwrap();
        session.campaign().unwrap();
        if before_stop == 1 {
            session.poll().unwrap();
            assert_eq!(session.status().committed_index, 1);
            // The no-op is durable, but poll just queued the readiness read.
            assert!(matches!(
                session.checkpoint(),
                Err(LedgerError::Consensus(ConsensusError::CheckpointIndex))
            ));
        }
        let budget = MemoryBudget::new(64 * 1024 * 1024, 24 * 1024 * 1024).unwrap();
        let (sender, _receiver) = mpsc::sync_channel(4);
        let (outbound, _outgoing) = hyper_rt::sync::channel(4).unwrap();
        let (host, mut owner) = ReplicaHost::assemble(
            session,
            ReplicaConfig::new(RootCommandId::from_u128(912)),
            ReplicaHost::wire_limits(),
            None,
            budget,
            HostSender::Direct(sender),
            outbound,
        )
        .unwrap();
        // Call exactly the same owned Stop handler used by direct/group hosts,
        // without allowing an initial worker tick to hide the readiness window.
        let (send, mut receive) = hyper_rt::sync::oneshot().unwrap();
        assert!(owner.accept(Work::Stop(send)).unwrap());
        receive.blocking_recv().unwrap().unwrap();
        assert_eq!(owner.session.status().committed_index, 1);
        assert_eq!(owner.session.sequence(), SessionSeq(0));
        owner.close();
        assert!(host.progress().stopped);
        drop(owner);
        drop(host);
        let mut recovered = Session::open(
            directory.path(),
            ledger(),
            config(),
            SessionLimits::default(),
        )
        .unwrap();
        assert_eq!(recovered.status().committed_index, 1);
        assert_eq!(recovered.sequence(), SessionSeq(0));
        recovered.campaign().unwrap();
        for _ in 0..3 {
            recovered.poll().unwrap();
        }
        let input = AuthenticatedInput {
            ledger: ledger(),
            principal: ParticipantId::from_u128(913),
            request_epoch: RequestEpoch(1),
            request_id: RequestId::from_u128(1),
            expected_revision: None,
            authority: AuthorityContext {
                runtime: true,
                cause: Cause::Root(RootCommandId::from_u128(912)),
                policy_revision: 1,
                logical_time: 0,
                evidence: vec![],
            },
            command: Command::NegotiateEpoch {
                epoch: RequestEpoch(1),
            },
        };
        assert!(matches!(
            recovered.submit_local(&input).unwrap(),
            Submission::Committed(_)
        ));
        assert_eq!(recovered.sequence(), SessionSeq(1));
    }
}

/// `closed` waits for the session to be let go, not for `stopped`: a stop
/// publishes `stopped` before its owner drops the session, and a session
/// opened in its place on `stopped` alone found the old log still leased on
/// the shared WAL (`LogicalLocked`, the managed service's replacement install
/// on a loaded machine). Driven by hand, the window between the two is held
/// open for as long as the test likes.
#[test]
fn closed_completes_once_the_session_is_let_go_not_once_stopped_is_published() {
    use focal_consensus::{ConsensusError, DurableNode};
    use focal_log::{LogError, SharedWal, WalIdentity, WalOptions, WalWriterLimits};
    use std::future::Future as _;
    fn open(wal: &SharedWal, tenant: &MemoryBudget) -> Result<Session, ConsensusError> {
        let node = DurableNode::open_on_wal_in(config(), wal.clone(), tenant)?;
        Ok(Session::from_node_in(ledger(), node, SessionLimits::default(), tenant).unwrap())
    }
    let directory = tempfile::tempdir().unwrap();
    let budget = MemoryBudget::new(256 * 1024 * 1024, 64 * 1024 * 1024).unwrap();
    let wal = SharedWal::open_with_budget(
        directory.path().join("wal"),
        WalOptions::new(WalIdentity {
            node: 1,
            cluster: [91; 16],
            stream: 0,
        }),
        WalWriterLimits::default(),
        budget.child(64 * 1024 * 1024, 16 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    let tenant = budget.child(64 * 1024 * 1024, 16 * 1024 * 1024).unwrap();
    let mut session = open(&wal, &tenant).unwrap();
    session.campaign().unwrap();
    let (sender, _receiver) = mpsc::sync_channel(4);
    let (outbound, _outgoing) = hyper_rt::sync::channel(4).unwrap();
    let (host, mut owner) = ReplicaHost::assemble(
        session,
        ReplicaConfig::new(RootCommandId::from_u128(912)),
        ReplicaHost::wire_limits(),
        None,
        budget.clone(),
        HostSender::Direct(sender),
        outbound,
    )
    .unwrap();
    let (send, mut receive) = hyper_rt::sync::oneshot().unwrap();
    assert!(owner.accept(Work::Stop(send)).unwrap());
    receive.blocking_recv().unwrap().unwrap();
    owner.close();
    assert!(host.progress().stopped);
    focal_rt::block_on(async move {
        let mut closed = std::pin::pin!(host.closed());
        // Stopped, and the owner still holds the session: its log is leased.
        let pending = std::future::poll_fn(|context| {
            std::task::Poll::Ready(closed.as_mut().poll(context).is_pending())
        })
        .await;
        assert!(pending);
        assert!(matches!(
            open(&wal, &tenant),
            Err(ConsensusError::Log(LogError::LogicalLocked))
        ));
        drop(owner);
        closed.await.unwrap();
        let mut reopened = open(&wal, &tenant).unwrap();
        reopened.campaign().unwrap();
    })
    .unwrap();
}
