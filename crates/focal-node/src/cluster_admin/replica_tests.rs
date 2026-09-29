use super::*;
use focal_consensus::NodeConfig;
use focal_ledger::{Session, SessionLimits};

#[test]
fn actual_receipt_recovery_and_later_fence_preserve_distinct_unknown_outcomes() {
    let disk = tempfile::tempdir().unwrap();
    crate::set_test_mode(disk.path(), 0o700);
    let mut settings = Settings::default();
    settings.node.data_dir = Some(disk.path().into());
    drop(crate::embedded::EmbeddedNode::open(&settings).unwrap());
    let admin = ClusterAdmin::open(&settings).unwrap();
    let wal = tempfile::tempdir().unwrap();
    let group = [81; 16];
    let mut session = Session::open(
        wal.path(),
        admin.identity.ledger,
        NodeConfig::single(admin.identity.node, admin.identity.cluster, group),
        SessionLimits::default(),
    )
    .unwrap();
    session.campaign().unwrap();
    for _ in 0..4 {
        session.poll().unwrap();
    }
    let view = session.membership().unwrap();
    let learner = admin.identity.node.checked_add(1).unwrap();
    let request = SessionMembershipRequest {
        id: [82; 16],
        expected_index: view.configuration_index,
        expected: view.configuration,
        change: MembershipChange::AddLearner { node: learner },
    };
    let (mut journal, mut saved) = admin.replica_journal(true).unwrap();
    saved.next = 2;
    saved.latest = Some(Intent {
        sequence: 1,
        session: admin.identity.ledger.session,
        group,
        request: request.clone(),
        receipt: None,
        fenced: false,
    });
    save_value(&mut journal, &saved).unwrap();
    assert!(matches!(
        admin.replica_journal(false),
        Err(ClusterAdminError::Journal(
            focal_enrollment::EnrollmentError::Locked
        ))
    ));
    assert!(!reconcile_view(&mut saved, &session.membership().unwrap()).unwrap());
    session.propose_membership(&request).unwrap();
    assert!(session.membership_receipt(&request).unwrap().is_none());
    for _ in 0..4 {
        session.poll().unwrap();
    }
    let committed = session.membership().unwrap();
    drop(journal); // Death after the server committed and before its local reply was saved.
    let (mut journal, mut reopened) = admin.replica_journal(false).unwrap();
    assert!(reopened.latest.as_ref().unwrap().receipt.is_none());
    assert!(reconcile_view(&mut reopened, &committed).unwrap());
    save_value(&mut journal, &reopened).unwrap();
    let exact = result(&admin.identity, &reopened).unwrap();
    assert!(
        matches!(&exact,AdminResult::ReplicaCommitted{committed_index,..} if *committed_index==committed.configuration_index)
    );
    drop(journal);
    assert_eq!(admin.replica_inspect().unwrap(), exact);

    // A different, real operation replaces Session's one retained receipt.
    let second = SessionMembershipRequest {
        id: [83; 16],
        expected_index: committed.configuration_index,
        expected: committed.configuration.clone(),
        change: MembershipChange::Remove { node: learner },
    };
    session.propose_membership(&second).unwrap();
    for _ in 0..4 {
        session.poll().unwrap();
    }
    assert_eq!(session.sequence().0, 0);
    let (mut journal, mut reopened) = admin.replica_journal(false).unwrap();
    reopened.latest.as_mut().unwrap().receipt = None;
    save_value(&mut journal, &reopened).unwrap();
    assert!(reconcile_view(&mut reopened, &session.membership().unwrap()).unwrap());
    save_value(&mut journal, &reopened).unwrap();
    assert!(
        matches!(result(&admin.identity,&reopened).unwrap(),AdminResult::ReplicaRequest{state,..} if state=="FencedOutcomeUnknown")
    );
    assert!(
        !reconcile_view(&mut reopened, &committed).unwrap(),
        "a delayed older view cannot undo the durable fence"
    );
    drop(journal);
    fs::remove_file(admin.root.join(DIRECTORY).join("journal.bin")).unwrap();
    assert!(matches!(
        admin.replica_journal(true),
        Err(ClusterAdminError::Corrupt) | Err(ClusterAdminError::Journal(_))
    ));
    fs::remove_dir_all(admin.root.join(DIRECTORY)).unwrap();
    assert!(matches!(
        admin.replica_journal(true),
        Err(ClusterAdminError::Corrupt)
    ));
}
