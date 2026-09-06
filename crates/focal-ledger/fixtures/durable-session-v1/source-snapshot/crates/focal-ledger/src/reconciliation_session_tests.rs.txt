use std::collections::BTreeSet;

fn reconciliation_query(epoch: u64, request: u128) -> ReconcileQuery {
    ReconcileQuery::Receipt {
        epoch: RequestEpoch(epoch),
        request: RequestId::from_u128(request),
    }
}

fn reconciled(session: &Session, query: &ReconcileQuery) -> ReconcilePage {
    session
        .reconcile_at_least(
            identity(),
            ParticipantId::from_u128(1),
            query,
            session.sequence(),
        )
        .unwrap()
        .to_owned()
        .unwrap()
}

#[test]
fn reconciliation_observes_only_published_requests_and_never_appends() {
    let dir = tempfile::tempdir().unwrap();
    let mut session =
        Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
    elect(&mut session);
    let request = input(
        1,
        Command::NegotiateEpoch {
            epoch: RequestEpoch(1),
        },
    );
    assert!(matches!(
        session.propose(&request).unwrap(),
        Submission::Pending(_)
    ));
    let status = session.status();
    let memory = session.memory_stats().used;
    let before = session.core.encode_checkpoint().unwrap();
    let query = reconciliation_query(1, 1);
    let page = reconciled(&session, &query);
    assert_eq!(page.sequence, SessionSeq(0));
    assert!(matches!(
        page.result,
        ReconcileResult::Receipt {
            epoch: EpochReconciliation {
                minimum: None,
                latest_admitted: None,
                admitted: false,
                ..
            },
            resolution: ReceiptResolution::Unknown,
            ..
        }
    ));
    assert!(matches!(
        session.reconcile_at_least(identity(), request.principal, &query, SessionSeq(1)),
        Err(LedgerError::Behind)
    ));
    assert_eq!(session.status().applied_index, status.applied_index);
    assert_eq!(session.status().committed_index, status.committed_index);
    assert_eq!(session.core.encode_checkpoint().unwrap(), before);
    assert_eq!(session.memory_stats().used, memory);
    assert_eq!(session.pending_count(), 1);

    let events = session.poll().unwrap();
    assert_eq!(session.sequence(), SessionSeq(1));
    let page = reconciled(&session, &query);
    assert_eq!(page.sequence, SessionSeq(1));
    let retained = session
        .receipt(&RequestKey {
            principal: request.principal,
            epoch: request.request_epoch,
            id: request.request_id,
        })
        .unwrap();
    assert!(
        matches!(page.result, ReconcileResult::Receipt { resolution: ReceiptResolution::Committed(receipt), .. } if *receipt == *retained)
    );
    drop(events);
}

#[test]
fn reconciliation_floor_and_retained_results_recover_through_checkpoint_and_wal_tail() {
    let dir = tempfile::tempdir().unwrap();
    let before_restart;
    let epoch_one;
    {
        let mut session =
            Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
        elect(&mut session);
        epoch_one = match session
            .submit_local(&input(
                1,
                Command::NegotiateEpoch {
                    epoch: RequestEpoch(1),
                },
            ))
            .unwrap()
        {
            Submission::Committed(receipt) => receipt,
            other => panic!("unexpected {other:?}"),
        };
        let mut second = input(
            2,
            Command::NegotiateEpoch {
                epoch: RequestEpoch(2),
            },
        );
        second.request_epoch = RequestEpoch(2);
        session.submit_local(&second).unwrap();
        session.checkpoint().unwrap();
        let mut floor = input(
            3,
            Command::AdvanceEpochFloor {
                minimum: RequestEpoch(2),
            },
        );
        floor.request_epoch = RequestEpoch(2);
        assert!(matches!(
            session.propose(&floor).unwrap(),
            Submission::Pending(_)
        ));
        assert!(matches!(
            reconciled(&session, &reconciliation_query(1, 77)).result,
            ReconcileResult::Receipt {
                resolution: ReceiptResolution::Unknown,
                ..
            }
        ));
        session.poll().unwrap();
        before_restart = [
            reconciliation_query(1, 1),
            reconciliation_query(1, 77),
            reconciliation_query(2, 77),
            reconciliation_query(3, 77),
        ]
        .map(|query| reconciled(&session, &query));
        assert!(
            matches!(&before_restart[0].result, ReconcileResult::Receipt { resolution: ReceiptResolution::Committed(receipt), .. } if **receipt == epoch_one)
        );
        assert!(matches!(
            before_restart[1].result,
            ReconcileResult::Receipt {
                resolution: ReceiptResolution::BelowFloor {
                    minimum: RequestEpoch(2)
                },
                ..
            }
        ));
    }
    let mut session =
        Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
    // Recovery can be locally inspected, but this accessor does not claim that a
    // new quorum barrier exists. The remote owner must establish its own barrier.
    assert!(!session.is_authoritative());
    assert_eq!(session.sequence(), SessionSeq(3));
    for (query, expected) in [
        reconciliation_query(1, 1),
        reconciliation_query(1, 77),
        reconciliation_query(2, 77),
        reconciliation_query(3, 77),
    ]
    .into_iter()
    .zip(before_restart)
    {
        assert_eq!(reconciled(&session, &query), expected);
    }
    elect(&mut session);
    assert_eq!(
        session
            .propose(&input(
                1,
                Command::NegotiateEpoch {
                    epoch: RequestEpoch(1)
                }
            ))
            .unwrap(),
        Submission::Committed(epoch_one)
    );
    assert_eq!(session.sequence(), SessionSeq(3));
}

#[test]
fn reconciliation_cursor_receipts_are_original_across_advances_floor_and_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let original;
    let registration;
    let expected;
    {
        let mut session =
            Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
        elect(&mut session);
        session.submit_local(&epoch(1)).unwrap();
        registration = cursor_input(
            &session,
            10,
            CursorOperation::RegisterProtected {
                consumer: ConsumerId::from_u128(1),
                scope: ContentHash([8; 32]),
                filter: DeltaFilter::Claims(BTreeSet::from([
                    ClaimId::from_u128(90),
                    ClaimId::from_u128(80),
                ])),
                start: Position::origin(identity()),
            },
        );
        assert!(matches!(
            session.submit_cursor_control(&registration).unwrap(),
            CursorSubmission::Pending(_)
        ));
        assert!(matches!(
            reconciled(&session, &reconciliation_query(1, 10)).result,
            ReconcileResult::Receipt {
                resolution: ReceiptResolution::Unknown,
                ..
            }
        ));
        // Even pending cross-family reuse conflicts, while the read remains unknown.
        assert!(matches!(
            session
                .propose(&input(
                    10,
                    Command::NegotiateEpoch {
                        epoch: RequestEpoch(1)
                    }
                ))
                .unwrap(),
            Submission::Domain(DomainOutcome::Refuse {
                code: ErrorCode::IdempotencyConflict,
                ..
            })
        ));
        session.poll().unwrap();
        session.submit_local(&epoch(2)).unwrap();
        let first_ack = ack(&session, 11, Position::resolved(identity(), SessionSeq(1)));
        original = cursor_receipt(session.submit_cursor_local(&first_ack).unwrap());
        session.checkpoint().unwrap();
        let later_ack = ack(&session, 12, Position::resolved(identity(), SessionSeq(2)));
        session.submit_cursor_local(&later_ack).unwrap();
        let mut floor = input(
            13,
            Command::AdvanceEpochFloor {
                minimum: RequestEpoch(2),
            },
        );
        floor.request_epoch = RequestEpoch(2);
        session.submit_local(&floor).unwrap();
        expected = reconciled(&session, &reconciliation_query(1, 11));
        let original_record = original.record.as_ref().unwrap();
        assert_ne!(
            session
                .cursor(ConsumerId::from_u128(1))
                .unwrap()
                .token
                .position,
            original_record.token.position
        );
        let ReconcileResult::Receipt {
            epoch,
            resolution: ReceiptResolution::CommittedCursor(dto),
            ..
        } = &expected.result
        else {
            panic!("{expected:?}")
        };
        assert!(!epoch.admitted);
        assert_eq!(epoch.minimum, Some(RequestEpoch(2)));
        assert_eq!(
            postcard::to_stdvec(&original).unwrap(),
            postcard::to_stdvec(dto.as_ref()).unwrap()
        );
        assert_eq!(
            dto.record.as_ref().unwrap().filter,
            CursorFilterSnapshot::Claims(vec![ClaimId::from_u128(80), ClaimId::from_u128(90)])
        );
        let view = session
            .reconcile_at_least(
                identity(),
                registration.key.principal,
                &reconciliation_query(1, 11),
                SessionSeq(3),
            )
            .unwrap();
        assert_eq!(view.item_count(), 2);
        assert!(
            view.owned_bytes().unwrap()
                >= size_of::<ReconcilePage>()
                    + size_of::<CursorMutationReceipt>()
                    + 2 * size_of::<ClaimId>()
        );
        assert!(matches!(
            session
                .reconcile_at_least(
                    identity(),
                    ParticipantId::from_u128(2),
                    &reconciliation_query(1, 11),
                    SessionSeq(3)
                )
                .unwrap()
                .to_owned()
                .unwrap()
                .result,
            ReconcileResult::Receipt {
                resolution: ReceiptResolution::Unknown,
                ..
            }
        ));
    }
    let mut session =
        Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
    assert_eq!(reconciled(&session, &reconciliation_query(1, 11)), expected);
    let ReconcileResult::Receipt {
        resolution: ReceiptResolution::CommittedCursor(dto),
        ..
    } = reconciled(&session, &reconciliation_query(1, 10)).result
    else {
        panic!("missing original registration")
    };
    assert_eq!(dto.record.unwrap().token.position.sequence, SessionSeq(0));
    assert_eq!(session.cursor_receipt(&original.key).unwrap(), &original);
    elect(&mut session);
    assert!(matches!(
        session.submit_cursor_control(&registration).unwrap(),
        CursorSubmission::Committed(_)
    ));
    assert_eq!(session.sequence(), SessionSeq(3));
    assert!(matches!(
        session
            .propose(&input(
                10,
                Command::NegotiateEpoch {
                    epoch: RequestEpoch(1)
                }
            ))
            .unwrap(),
        Submission::Domain(DomainOutcome::Refuse {
            code: ErrorCode::IdempotencyConflict,
            ..
        })
    ));
}
