use super::*;
use crate::tests::{ISSUER, WORKER, input, ledger, new_claim};

fn commit(core: &mut Core, request: &AuthenticatedInput) -> MutationReceipt {
    let prepared = core.prepare(request).unwrap();
    core.apply(SessionSeq(core.sequence().0 + 1), prepared)
        .unwrap()
        .receipt
}

fn negotiate(epoch: u64) -> AuthenticatedInput {
    let mut request = input(
        u128::from(epoch),
        ISSUER,
        Command::NegotiateEpoch {
            epoch: RequestEpoch(epoch),
        },
    );
    request.request_epoch = RequestEpoch(epoch);
    request
}

fn page(core: &Core, epoch: u64, request: u128) -> ReconcilePage {
    core.reconcile(
        ledger(),
        ISSUER,
        &ReconcileQuery::Receipt {
            epoch: RequestEpoch(epoch),
            request: RequestId::from_u128(request),
        },
    )
    .unwrap()
    .to_owned()
    .unwrap()
}

#[test]
fn reconciliation_pending_inputs_are_unknown_until_applied() {
    let mut core = Core::new(ledger(), Limits::default());
    let request = negotiate(1);
    let prepared = core.prepare(&request).unwrap();
    let before = core.encode_checkpoint().unwrap();
    let expected_epoch = EpochReconciliation {
        epoch: RequestEpoch(1),
        minimum: None,
        latest_admitted: None,
        admitted: false,
    };
    assert!(
        matches!(page(&core, 1, 1).result, ReconcileResult::Receipt {
        epoch, resolution: ReceiptResolution::Unknown, ..
    } if epoch == expected_epoch)
    );
    assert_eq!(
        core.reconcile(
            ledger(),
            ISSUER,
            &ReconcileQuery::Epoch {
                epoch: RequestEpoch(1)
            }
        )
        .unwrap()
        .result,
        ReconcileResultView::Epoch(expected_epoch),
    );
    assert_eq!(core.encode_checkpoint().unwrap(), before);
    let receipt = core.apply(SessionSeq(1), prepared).unwrap().receipt;
    let found = page(&core, 1, 1);
    assert_eq!(found.sequence, SessionSeq(1));
    assert_eq!(found.schema, RECONCILE_SCHEMA);
    assert_eq!(found.ledger, ledger());
    assert_eq!(found.principal, ISSUER);
    assert!(matches!(found.result, ReconcileResult::Receipt {
        key, epoch: EpochReconciliation { minimum: Some(RequestEpoch(1)), latest_admitted: Some(RequestEpoch(1)), admitted: true, .. },
        resolution: ReceiptResolution::Committed(actual),
    } if key == receipt.key && *actual == receipt));
}

#[test]
fn reconciliation_floor_fences_absence_but_preserves_exact_receipts_and_checkpoint() {
    let mut core = Core::new(ledger(), Limits::default());
    let old = commit(&mut core, &negotiate(1));
    commit(&mut core, &negotiate(2));
    let mut advance = input(
        3,
        ISSUER,
        Command::AdvanceEpochFloor {
            minimum: RequestEpoch(2),
        },
    );
    advance.request_epoch = RequestEpoch(2);
    let prepared = core.prepare(&advance).unwrap();
    assert!(matches!(
        page(&core, 1, 77).result,
        ReconcileResult::Receipt {
            resolution: ReceiptResolution::Unknown,
            ..
        }
    ));
    core.apply(SessionSeq(3), prepared).unwrap();

    let checkpoint = core.encode_checkpoint().unwrap();
    let recovered = Core::decode_checkpoint(&checkpoint).unwrap();
    for owner in [&core, &recovered] {
        assert!(
            matches!(page(owner, 1, 1).result, ReconcileResult::Receipt {
            epoch: EpochReconciliation { minimum: Some(RequestEpoch(2)), latest_admitted: Some(RequestEpoch(2)), admitted: false, .. },
            resolution: ReceiptResolution::Committed(receipt), ..
        } if *receipt == old)
        );
        assert!(matches!(
            page(owner, 1, 77).result,
            ReconcileResult::Receipt {
                resolution: ReceiptResolution::BelowFloor {
                    minimum: RequestEpoch(2)
                },
                ..
            }
        ));
        for epoch in [2, 3, u64::MAX] {
            assert!(
                matches!(page(owner, epoch, 77).result, ReconcileResult::Receipt {
                epoch: observation, resolution: ReceiptResolution::Unknown, ..
            } if observation.admitted == (epoch == 2))
            );
        }
        // A retained receipt can still resolve an exact retry, despite its floor.
        assert!(
            matches!(owner.prepare(&negotiate(1)), Err(DomainOutcome::Duplicate(receipt)) if *receipt == old)
        );
        let mut unknown = negotiate(1);
        unknown.request_id = RequestId::from_u128(77);
        assert!(matches!(
            owner.prepare(&unknown),
            Err(DomainOutcome::Refuse {
                code: ErrorCode::RequestHistoryExpired,
                ..
            })
        ));
        assert_eq!(owner.encode_checkpoint().unwrap(), checkpoint);
    }
}

#[test]
fn reconciliation_is_principal_and_ledger_scoped_and_rejects_invalid_keys() {
    let mut core = Core::new(ledger(), Limits::default());
    commit(&mut core, &negotiate(1));
    let query = ReconcileQuery::Receipt {
        epoch: RequestEpoch(1),
        request: RequestId::from_u128(1),
    };
    let other = core
        .reconcile(ledger(), WORKER, &query)
        .unwrap()
        .to_owned()
        .unwrap();
    assert_eq!(other.principal, WORKER);
    assert!(matches!(other.result, ReconcileResult::Receipt {
        key, epoch: EpochReconciliation { minimum: None, latest_admitted: None, admitted: false, .. },
        resolution: ReceiptResolution::Unknown,
    } if key.principal == WORKER));
    let wrong = LedgerId {
        session: SessionId::from_u128(90),
        ..ledger()
    };
    assert_eq!(
        core.reconcile(wrong, ISSUER, &query).unwrap_err(),
        ReconciliationError::LedgerMismatch
    );
    assert_eq!(
        core.reconcile(ledger(), ParticipantId::from_u128(0), &query)
            .unwrap_err(),
        ReconciliationError::InvalidIdentity
    );
    for query in [
        ReconcileQuery::Epoch {
            epoch: RequestEpoch(0),
        },
        ReconcileQuery::Receipt {
            epoch: RequestEpoch(0),
            request: RequestId::from_u128(1),
        },
    ] {
        assert_eq!(
            core.reconcile(ledger(), ISSUER, &query).unwrap_err(),
            ReconciliationError::InvalidEpoch
        );
    }
    assert_eq!(
        core.reconcile(
            ledger(),
            ISSUER,
            &ReconcileQuery::Receipt {
                epoch: RequestEpoch(1),
                request: RequestId::from_u128(0)
            }
        )
        .unwrap_err(),
        ReconciliationError::InvalidIdentity
    );
}

#[test]
fn reconciliation_generated_receipt_copy_accounts_for_owned_vectors() {
    let mut core = Core::new(ledger(), Limits::default());
    commit(&mut core, &negotiate(1));
    let request = input(
        20,
        ISSUER,
        Command::GenerateClaimBatch {
            claims: (100..116).map(new_claim).collect(),
        },
    );
    let receipt = commit(&mut core, &request);
    let view = core
        .reconcile(
            ledger(),
            ISSUER,
            &ReconcileQuery::Receipt {
                epoch: RequestEpoch(1),
                request: request.request_id,
            },
        )
        .unwrap();
    let ids = match &receipt.outcome {
        CommandResult::Generated(ids) => ids,
        other => panic!("unexpected {other:?}"),
    };
    let page = view.to_owned().unwrap();
    assert!(
        view.owned_bytes().unwrap()
            >= std::mem::size_of::<ReconcilePage>()
                + std::mem::size_of::<MutationReceipt>()
                + ids.len() * std::mem::size_of::<ClaimId>()
    );
    assert!(
        matches!(page.result, ReconcileResult::Receipt { resolution: ReceiptResolution::Committed(found), .. } if *found == receipt)
    );
    assert_eq!(core.sequence(), SessionSeq(2));
}
