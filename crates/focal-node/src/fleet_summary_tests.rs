use super::*;

fn summary(reply: ResponseEnvelope) -> LedgerSummary {
    match reply.result {
        Response::Summary(value) => value,
        other => panic!("{other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scalar_summary_requires_current_quorum_survives_restart_and_never_consumes_a_request_key()
{
    let root = tempfile::tempdir().unwrap();
    let fleet = Fleet::open(root.path());
    let leader = fleet.leader(None).await;
    let read = request(60_001, Operation::Summary);
    let empty = summary(fleet.retry_exact(leader, &read).await);
    assert_eq!(empty.claims, 0);
    assert_eq!(empty.token.sequence, SessionSeq(0));
    let epoch = request(
        60_001,
        Operation::OpenEpoch {
            epoch: RequestEpoch(1),
        },
    );
    let Response::Submitted(MutationReply::Committed(receipt)) =
        fleet.retry_exact(leader, &epoch).await.result
    else {
        panic!("read consumed mutation key")
    };
    fleet.all_at(receipt.sequence).await;
    let current = summary(fleet.retry_exact(leader, &read).await);
    assert_eq!(current.token.sequence, receipt.sequence);
    assert_eq!(
        (
            current.claims,
            current.testaments,
            current.artifacts,
            current.validations,
            current.evidence_sets,
            current.validation_runs
        ),
        (0, 0, 0, 0, 0, 0)
    );
    assert_eq!(summary(fleet.retry_exact(leader, &read).await), current);
    let peer = AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId::from_u128(999),
        tenants: [TenantId::from_u128(99)].into_iter().collect(),
        role: PeerRole::Actor,
    })
    .unwrap();
    assert!(matches!(
        dispatch(
            &fleet.hosts[leader],
            peer,
            read.clone(),
            &ReplicaHost::wire_limits()
        )
        .await
        .result,
        Response::Error(AccessError::Unauthorized)
    ));
    fleet.isolated.store(leader as u8 + 1, Ordering::SeqCst);
    assert!(matches!(
        dispatch(
            &fleet.hosts[leader],
            actor(),
            read.clone(),
            &ReplicaHost::wire_limits()
        )
        .await
        .result,
        Response::Error(AccessError::Unavailable)
    ));
    let replacement = fleet.leader(Some(leader)).await;
    let next = summary(fleet.retry_exact(replacement, &read).await);
    assert_eq!(next.token, current.token);
    assert!(next.applied_index >= current.applied_index);
    fleet.isolated.store(0, Ordering::SeqCst);
    fleet.all_at(receipt.sequence).await;
    fleet.stop().await;
    let reopened = Fleet::open(root.path());
    let leader = reopened.leader(None).await;
    let next = summary(reopened.retry_exact(leader, &read).await);
    assert_eq!(next.token, current.token);
    assert_eq!(next.claims, 0);
    reopened.stop().await;
}
