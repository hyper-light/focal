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
        let (outbound, _outgoing) = async_mpsc::channel(4);
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
        let (send, receive) = oneshot::channel();
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
