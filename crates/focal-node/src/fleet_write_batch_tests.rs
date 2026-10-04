//! A batch of requests an owner takes is written at once (27 §9): its
//! proposals go in one write, not one write each.
use super::*;

/// One participant request opening the issuer's epoch under its own id: a
/// proposal of its own.
fn opening(fixture: &mut Fixture, id: u128) -> Work {
    let owner = &mut fixture.owners[0];
    let request = RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: identity().ledger,
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(id),
        operation: Operation::OpenEpoch {
            epoch: RequestEpoch(1),
        },
    };
    let verified = verify_request(actor(), request, &owner.client_limits).unwrap();
    let charge = owner
        .budget
        .reserve(BudgetKind::Pending, BudgetLane::Completion, 64 * 1024)
        .unwrap()
        .commit();
    let (send, _answered) = oneshot::channel();
    Work::Request(
        Box::new(AdmittedRequest {
            verified,
            witness: None,
            native: None,
        }),
        send,
        charge,
    )
}

/// The leader's owner takes four proposals as a batch and writes them in
/// one group commit; taken one by one, each starts a write of its own. An
/// owner that drained for every proposal as it came carried one proposal a
/// write, and a session with a write out is dispatched nothing more, so a
/// group committing six entries a second held 28 requests queued behind it
/// (six fleets at once, 2026-10-03).
#[test]
fn an_owner_writes_the_proposals_of_a_batch_at_once() {
    let root = tempfile::tempdir().unwrap();
    let mut fixture = Fixture::open(root.path());
    let wal = fixture.owners[0].session.shared_wal();
    let commits = || wal.stats().unwrap().group_commits;
    // One at a time: each proposal its own write.
    let before = commits();
    let last = fixture.owners[0].session.last_log_index().unwrap();
    for id in 900..902 {
        let work = opening(&mut fixture, id);
        assert!(!fixture.owners[0].accept(work).unwrap());
    }
    assert_eq!(
        fixture.owners[0].session.last_log_index().unwrap() - last,
        2,
        "two proposals"
    );
    assert_eq!(commits() - before, 2, "taken one by one: a write each");
    // A batch: four proposals, one write at the batch's drain.
    let before = commits();
    let last = fixture.owners[0].session.last_log_index().unwrap();
    fixture.owners[0].batching = true;
    for id in 910..914 {
        let work = opening(&mut fixture, id);
        assert!(!fixture.owners[0].accept(work).unwrap());
    }
    assert_eq!(
        commits(),
        before,
        "nothing written before the batch's drain"
    );
    fixture.owners[0].batching = false;
    fixture.owners[0].drain().unwrap();
    assert_eq!(
        fixture.owners[0].session.last_log_index().unwrap() - last,
        4,
        "four proposals"
    );
    assert_eq!(commits() - before, 1, "a batch: one write");
}
