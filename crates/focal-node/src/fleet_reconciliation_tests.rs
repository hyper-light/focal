use super::*;

fn lookup(id: u128) -> RequestEnvelope {
    request(
        50_000,
        Operation::Reconcile(ReconcileQuery::Receipt {
            epoch: RequestEpoch(1),
            request: RequestId::from_u128(id),
        }),
    )
}
fn page(reply: ResponseEnvelope) -> ReconcileReply {
    match reply.result {
        Response::Reconciled(page) => page,
        other => panic!("{other:?}"),
    }
}
fn same_page(actual: ReconcileReply, before: &ReconcileReply) {
    assert_eq!(actual.page, before.page);
    assert_eq!(actual.token, before.token);
    assert!(actual.applied_index >= before.applied_index);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn receipt_reads_require_live_quorum_preserve_privacy_and_recover_after_leader_restart() {
    let root = tempfile::tempdir().unwrap();
    let fleet = Fleet::open(root.path());
    let leader = fleet.leader(None).await;
    let mutation = request(
        101,
        Operation::OpenEpoch {
            epoch: RequestEpoch(1),
        },
    );
    let committed = fleet.retry_exact(leader, &mutation).await;
    let Response::Submitted(MutationReply::Committed(expected)) = committed.result else {
        panic!("{committed:?}")
    };
    fleet.all_at(expected.sequence).await;
    let queried = lookup(101);
    let found = page(fleet.retry_exact(leader, &queried).await);
    assert_eq!(found.token.sequence, expected.sequence);
    assert_eq!(found.page.principal, actor().principal());
    assert!(
        matches!(found.page.result, ReconcileResult::Receipt { resolution:ReceiptResolution::Committed(ref actual),.. } if **actual==expected)
    );
    assert!(
        fleet
            .hosts
            .iter()
            .all(|host| host.progress().sequence == expected.sequence)
    );

    let stranger = AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId::from_u128(1000),
        tenants: [ledger().tenant].into_iter().collect(),
        role: PeerRole::Actor,
    })
    .unwrap();
    let hidden = page(
        dispatch(
            &fleet.hosts[leader],
            stranger.clone(),
            queried.clone(),
            &ReplicaHost::wire_limits(),
        )
        .await,
    );
    assert_eq!(hidden.page.principal, stranger.principal());
    assert!(matches!(
        hidden.page.result,
        ReconcileResult::Receipt {
            epoch: EpochReconciliation {
                admitted: false,
                minimum: None,
                ..
            },
            resolution: ReceiptResolution::Unknown,
            ..
        }
    ));
    assert!(matches!(
        dispatch(
            &fleet.hosts[leader],
            peer(1),
            queried.clone(),
            &ReplicaHost::wire_limits()
        )
        .await
        .result,
        Response::Error(AccessError::Unauthorized)
    ));

    fleet.isolated.store(leader as u8 + 1, Ordering::SeqCst);
    let stale = dispatch(
        &fleet.hosts[leader],
        actor(),
        queried.clone(),
        &ReplicaHost::wire_limits(),
    )
    .await;
    assert!(
        matches!(stale.result, Response::Error(AccessError::Unavailable)),
        "even a retained exact receipt needs a fresh quorum: {stale:?}"
    );
    let replacement = fleet.leader(Some(leader)).await;
    let after_failover = page(fleet.retry_exact(replacement, &queried).await);
    same_page(after_failover, &found);
    let absent = page(fleet.retry_exact(replacement, &lookup(102)).await);
    assert!(matches!(
        absent.page.result,
        ReconcileResult::Receipt {
            resolution: ReceiptResolution::Unknown,
            ..
        }
    ));
    // The observed absence does not consume this ID or prevent its later commit.
    let later = request(
        102,
        Operation::OpenEpoch {
            epoch: RequestEpoch(1),
        },
    );
    let committed_later = fleet.retry_exact(replacement, &later).await;
    let Response::Submitted(MutationReply::Committed(later_receipt)) = committed_later.result
    else {
        panic!("{committed_later:?}")
    };
    assert!(later_receipt.sequence > absent.page.sequence);
    let after_commit = page(fleet.retry_exact(replacement, &lookup(102)).await);
    assert!(
        matches!(after_commit.page.result,ReconcileResult::Receipt { resolution:ReceiptResolution::Committed(ref receipt),.. } if **receipt==later_receipt)
    );
    fleet.isolated.store(0, Ordering::SeqCst);
    fleet.all_at(later_receipt.sequence).await;
    let current = page(fleet.retry_exact(replacement, &queried).await);
    assert_eq!(current.page.sequence, later_receipt.sequence);
    let stream = request(
        103,
        Operation::Stream(StreamRequest::Open {
            consumer: focal_stream::ConsumerId::from_u128(103),
            filter: focal_stream::DeltaFilter::All,
            start: None,
            seed: false,
            credits: Credits {
                items: 1,
                bytes: 65536,
            },
        }),
    );
    assert!(matches!(
        fleet.retry_exact(replacement, &stream).await.result,
        Response::Stream(_)
    ));
    let cursor = page(fleet.retry_exact(replacement, &lookup(103)).await);
    assert_eq!(cursor.page.sequence, current.page.sequence);
    let ReconcileResult::Receipt {
        resolution: ReceiptResolution::CommittedCursor(ref cursor_receipt),
        ..
    } = cursor.page.result
    else {
        panic!("{cursor:?}")
    };
    assert!(cursor_receipt.raft_index > current.applied_index);
    assert!(cursor_receipt.raft_index <= cursor.applied_index);
    fleet.stop().await;
    let reopened = Fleet::open(root.path());
    let leader = reopened.leader(None).await;
    same_page(page(reopened.retry_exact(leader, &queried).await), &current);
    same_page(
        page(reopened.retry_exact(leader, &lookup(102)).await),
        &after_commit,
    );
    same_page(
        page(reopened.retry_exact(leader, &lookup(103)).await),
        &cursor,
    );
    let epoch = request(
        50_001,
        Operation::Reconcile(ReconcileQuery::Epoch {
            epoch: RequestEpoch(1),
        }),
    );
    assert!(matches!(
        page(reopened.retry_exact(leader, &epoch).await).page.result,
        ReconcileResult::Epoch(EpochReconciliation {
            admitted: true,
            minimum: Some(RequestEpoch(1)),
            ..
        })
    ));
    // Queries never create mutation receipts under their transport request IDs.
    let query_identity = page(reopened.retry_exact(leader, &lookup(50_000)).await);
    assert!(matches!(
        query_identity.page.result,
        ReconcileResult::Receipt {
            resolution: ReceiptResolution::Unknown,
            ..
        }
    ));
    reopened.stop().await;
}
