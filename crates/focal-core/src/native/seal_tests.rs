//! The audit's F12: a principal's request generations, the fences on them,
//! and the seals that move the closed generations' outcomes out of the live
//! core into bundles every replica derives alike.
use super::fixtures as fx;
use super::record_codec::recovery;
use super::record_codec::recovery::tests as ckpt;
use super::report_tests as f;
use super::seal::*;
use super::*;
use focal_evidence::BuiltinNativeSchemas;
use focal_model::{RequestEpoch, ValidationMode};

fn key(actor: ParticipantId, epoch: u64, id: u128) -> RequestKey {
    RequestKey {
        principal: actor,
        epoch: RequestEpoch(epoch),
        id: RequestId::from_u128(id),
    }
}
/// A creation the issuer sends in `epoch`.
fn creation_in(epoch: u64, id: u128, claim: u128) -> NativeInput {
    let mut input = f::creation(id, claim, &[], None);
    input.request = key(f::ISSUER, epoch, id);
    input
}
/// A floor advance `actor` sends in `epoch`.
fn advance(actor: ParticipantId, epoch: u64, id: u128, minimum: u64) -> NativeInput {
    NativeInput {
        request: key(actor, epoch, id),
        command: NativeCommand::AdvanceEpochFloor {
            minimum: RequestEpoch(minimum),
        },
    }
}
fn limits() -> record_codec::EncodingLimits {
    record_codec::EncodingLimits {
        bytes: 1 << 20,
        visits: 1 << 24,
        rows: 1 << 16,
    }
}
fn inspection() -> record_codec::InspectionLimits {
    record_codec::InspectionLimits {
        bytes: 1 << 20,
        visits: 1 << 24,
        rows: 1 << 16,
        row_bytes: 1 << 20,
    }
}
fn prepare(
    core: &mut Core<NativeState>,
    time: u64,
    input: NativeInput,
) -> Result<NativePreparation, NativeError> {
    core.prepare_native(f::context(input.request.principal, time), input, &[])
}
fn refused(result: Result<NativePreparation, NativeError>) -> NativeError {
    match result {
        Ok(_) => panic!("admitted"),
        Err(error) => error,
    }
}
fn window(core: &Core<NativeState>, actor: ParticipantId) -> EpochWindow {
    core.native_epochs(actor).unwrap().copy().unwrap()
}
fn meta(core: &Core<NativeState>) -> Meta {
    match core.state.rows.get(&Key::Meta) {
        Some(Row::Meta(meta)) => *meta,
        _ => panic!("meta row"),
    }
}
fn bound(core: &Core<NativeState>) -> SealBound {
    SealBound {
        principals: core.native_limits().principals,
        rows: DEFAULT_SEAL_ROWS_PER_BUNDLE,
    }
}
/// Derive, write and apply one seal as the authority does: the plan at the
/// committed prefix under `floors`, its bundle, the fold when named, and
/// the record every replica applies. The bundle and the fold's directory.
fn seal(
    core: &mut Core<NativeState>,
    floors: &[(ParticipantId, RequestEpoch)],
    fold: Option<(u64, u64)>,
) -> (SealPlan, Vec<u8>, ContentHash, Option<Vec<u8>>) {
    let plan = core.seal_plan(floors, bound(core)).unwrap();
    let quote = core.seal_quote(&plan, limits()).unwrap();
    let mut bytes = vec![0; quote.bytes];
    let hash = core.seal_into(&plan, &mut bytes, quote.visits).unwrap();
    assert_eq!(hash, quote.hash);
    assert!(bytes.starts_with(b"FCNSEAL1"));
    let fold = fold.map(|(first, last)| {
        let plan = core.fold_plan(first, last).unwrap();
        let quote = core.fold_quote(&plan, limits()).unwrap();
        let mut bytes = vec![0; quote.bytes];
        let hash = core.fold_into(&plan, &mut bytes, quote.visits).unwrap();
        assert_eq!(hash, quote.hash);
        (
            Fold {
                first,
                last,
                bundle: hash,
                bytes: bytes.len() as u64,
            },
            bytes,
        )
    });
    let record = SealRecord {
        floors,
        bound: SealBound {
            principals: plan.principals.len().max(1),
            rows: plan.rows(),
        },
        bundle: hash,
        bytes: bytes.len() as u64,
        count: plan.rows() as u64,
        fold: fold.as_ref().map(|(fold, _)| *fold),
    };
    let left = core.apply_seal(record).unwrap();
    assert_eq!(left, plan.rows());
    (plan, bytes, hash, fold.map(|(_, bytes)| bytes))
}
fn restore_with(
    core: &Core<NativeState>,
    store: &focal_evidence::ContentStore,
) -> Core<NativeState> {
    let encoded = ckpt::encode(core);
    let restored = recovery::restore(
        &ckpt::inspect(&encoded),
        RangeId(781),
        ckpt::limits(core.limits),
        ckpt::budget(),
        store,
        &BuiltinNativeSchemas,
    )
    .unwrap();
    ckpt::compare(core, &restored);
    restored
}

/// A principal's generations open in order by their first request, two at
/// once; a floor advance closes the ones below the generation it names and
/// is refused when it names the floor itself or more than the next; a fresh
/// request in a closed generation is refused by name while the exact retry
/// of a committed one is still answered from its resident outcome.
#[test]
fn a_principals_generations_open_in_order_and_close_at_the_floor() {
    let mut core = f::core();
    assert!(core.native_epochs(f::ISSUER).is_none());
    f::publish(&mut core, 10, creation_in(1, 1, 1));
    let opened = window(&core, f::ISSUER);
    assert_eq!(
        (opened.floor, opened.sealed, opened.open),
        (RequestEpoch(1), RequestEpoch(1), 1)
    );
    assert_eq!(
        opened.counts[0],
        OpenEpoch {
            outcomes: 1,
            last: 10
        }
    );
    assert_eq!(meta(&core).principals, 1);
    // Generation zero is no generation; three is not the next one.
    assert!(matches!(
        refused(prepare(&mut core, 11, creation_in(0, 2, 2))),
        NativeError::Contract(ContractError::InvalidTarget)
    ));
    assert!(matches!(
        refused(prepare(&mut core, 11, creation_in(3, 2, 2))),
        NativeError::Contract(ContractError::EpochNotAdmitted)
    ));
    f::publish(&mut core, 12, creation_in(2, 2, 2));
    let two = window(&core, f::ISSUER);
    assert_eq!(two.open, 2);
    assert_eq!(
        two.counts[1],
        OpenEpoch {
            outcomes: 1,
            last: 12
        }
    );
    assert_eq!(two.next(), RequestEpoch(3));
    // A third open generation waits for the floor.
    assert!(matches!(
        refused(prepare(&mut core, 13, creation_in(3, 3, 3))),
        NativeError::Contract(ContractError::EpochNotAdmitted)
    ));
    // The advance names a generation above the floor, at most the next.
    for minimum in [0, 1, 4] {
        assert!(matches!(
            refused(prepare(&mut core, 13, advance(f::ISSUER, 2, 3, minimum))),
            NativeError::Contract(ContractError::InvalidTransition)
        ));
    }
    let outcome = f::publish(&mut core, 13, advance(f::ISSUER, 2, 3, 2));
    assert_eq!(outcome.operation, NativeOperation::AdvanceEpochFloor);
    assert_eq!(
        core.native_outcome(NativeInvocation::Request(key(f::ISSUER, 2, 3))),
        Some(outcome)
    );
    let advanced = window(&core, f::ISSUER);
    assert_eq!(
        (advanced.floor, advanced.sealed, advanced.open),
        (RequestEpoch(2), RequestEpoch(1), 1)
    );
    assert_eq!(
        advanced.counts[0],
        OpenEpoch {
            outcomes: 2,
            last: 13
        }
    );
    assert_eq!(advanced.counts[1], OpenEpoch::default());
    assert_eq!(
        advanced.awaiting_seal(),
        Some((RequestEpoch(1), RequestEpoch(2)))
    );
    // Closed: a fresh request is refused; the committed one still answers.
    assert!(matches!(
        refused(prepare(&mut core, 14, creation_in(1, 4, 4))),
        NativeError::Contract(ContractError::RequestHistoryExpired)
    ));
    assert!(matches!(
        prepare(&mut core, 14, creation_in(1, 1, 1)),
        Ok(NativePreparation::Existing {
            committed: true,
            ..
        })
    ));
    // Generation three is the next one now.
    f::publish(&mut core, 15, creation_in(3, 5, 5));
    let three = window(&core, f::ISSUER);
    assert_eq!((three.floor, three.open), (RequestEpoch(2), 2));
    assert_eq!(
        three.counts[1],
        OpenEpoch {
            outcomes: 1,
            last: 15
        }
    );
    // A request never closes its own generation: the floor it asks for is
    // at most the generation it is sent in.
    assert!(matches!(
        refused(prepare(&mut core, 16, advance(f::ISSUER, 3, 6, 4))),
        NativeError::Contract(ContractError::InvalidTransition)
    ));
    f::publish(&mut core, 16, advance(f::ISSUER, 3, 6, 3));
    let closed = window(&core, f::ISSUER);
    assert_eq!((closed.floor, closed.open), (RequestEpoch(3), 1));
    assert_eq!(
        closed.counts[0],
        OpenEpoch {
            outcomes: 2,
            last: 16
        }
    );
    assert_eq!(closed.counts[1], OpenEpoch::default());
    assert_eq!(
        closed.awaiting_seal(),
        Some((RequestEpoch(1), RequestEpoch(3)))
    );
}

/// The live window is bounded for everyone first (`outcomes`), then shared
/// among the principals with one (`principal outcomes`): no principal takes
/// the window from the others, and the principals themselves are bounded.
#[test]
fn the_window_is_bounded_for_everyone_and_shared_among_principals() {
    let mut core = f::running(&[
        (ValidationMode::Required, false),
        (ValidationMode::Required, false),
    ]);
    assert_eq!(window(&core, f::ISSUER).open_outcomes(), 2);
    assert_eq!(window(&core, f::EVALUATOR).open_outcomes(), 2);
    assert_eq!(meta(&core).principals, 2);
    core.limits.outcomes = 6;
    f::publish(&mut core, 40, creation_in(1, 5, 5));
    assert_eq!(core.resident_outcomes(), 5);
    // The issuer's share of six among two is three: it holds them.
    assert!(matches!(
        refused(prepare(&mut core, 41, creation_in(1, 6, 6))),
        NativeError::Capacity("principal outcomes")
    ));
    // Under a bound the window has passed, everyone is refused by the bound
    // before any share.
    core.limits.outcomes = 4;
    assert!(matches!(
        refused(prepare(&mut core, 41, creation_in(1, 6, 6))),
        NativeError::Capacity("outcomes")
    ));
    assert!(matches!(
        refused(prepare(&mut core, 41, advance(f::EVALUATOR, 1, 60, 2))),
        NativeError::Capacity("outcomes")
    ));
    core.limits.outcomes = 64;
    // A principal never seen needs room among the principals: the subject
    // of a posted claim without admission requirements acquires its receipt.
    f::publish(&mut core, 43, creation_in(1, 7, 7));
    let posted = core.native_claim(ClaimId::from_u128(7)).unwrap().binding();
    f::publish(&mut core, 44, f::post(8, posted));
    let expected = core.native_claim(ClaimId::from_u128(7)).unwrap().binding();
    let acquire = || fx::acquire_receipt(key(f::SUBJECT, 1, 70), expected, 700);
    core.limits.principals = 2;
    assert!(matches!(
        refused(prepare(&mut core, 45, acquire())),
        NativeError::Capacity("principals")
    ));
    core.limits.principals = 3;
    f::publish(&mut core, 45, acquire());
    assert_eq!(meta(&core).principals, 3);
    let subject = window(&core, f::SUBJECT);
    assert_eq!((subject.floor, subject.open), (RequestEpoch(1), 1));
    assert_eq!(
        subject.counts[0],
        OpenEpoch {
            outcomes: 1,
            last: 45
        }
    );
}

/// A closed generation's outcomes seal into a bundle: the plan is derived
/// from the committed state, the bundle's inspector reads the outcome back,
/// the live core keeps only where it went, the exact retry of a sealed
/// request is refused by name, the checkpoint restores it all and an owner
/// rebuilds over it.
#[test]
fn a_closed_generation_seals_into_a_bundle_the_live_core_points_to() {
    let directory = tempfile::tempdir().unwrap();
    let store = ckpt::store(directory.path());
    let mut core = f::core();
    core.limits.pending = 1;
    core.limits.outcomes = 16;
    for claim in 1..=3u128 {
        f::publish(&mut core, 10 * claim as u64, creation_in(1, claim, claim));
    }
    let first = key(f::ISSUER, 1, 1);
    let before = core
        .native_outcome(NativeInvocation::Request(first))
        .unwrap();
    // Nothing is closed: nothing to seal, no pressure.
    assert_eq!(
        core.seal_plan(&[], bound(&core)).unwrap_err(),
        SealRefusal::Nothing
    );
    assert!(core.pressure_floors(8).unwrap().is_empty());
    f::publish(&mut core, 40, advance(f::ISSUER, 2, 4, 2));
    // Floors that are not the pressure floors of this state are refused.
    assert_eq!(
        core.seal_plan(&[(f::ISSUER, RequestEpoch(3))], bound(&core))
            .unwrap_err(),
        SealRefusal::Floors
    );
    let plan = core.seal_plan(&[], bound(&core)).unwrap();
    assert_eq!((plan.through, plan.ordinal), (SessionSeq(4), 1));
    assert_eq!(
        plan.principals,
        vec![SealedPrincipal {
            principal: f::ISSUER,
            first: RequestEpoch(1),
            last: RequestEpoch(1),
        }]
    );
    assert_eq!(plan.outcomes(), 3);
    // Deriving again yields the same plan: what every replica applies.
    assert_eq!(core.seal_plan(&[], bound(&core)).unwrap(), plan);
    let (plan, bytes, hash, _) = seal(&mut core, &[], None);
    // The bundle is read by its inspector: the header names the sealed
    // generations, the sealed outcome is read back, nothing else is.
    let sealed = record_codec::StructuralSeal::inspect(&bytes, inspection()).unwrap();
    assert_eq!(sealed.digest(), hash);
    let record_codec::SealHeader::Seal {
        ledger,
        through,
        ordinal,
        count,
        ..
    } = sealed.header()
    else {
        panic!("{:?}", sealed.header());
    };
    assert_eq!(
        (*ledger, *through, *ordinal, *count),
        (f::ledger(), SessionSeq(4), 1, plan.rows())
    );
    assert_eq!(
        sealed.header().member_of(f::ISSUER, RequestEpoch(1)),
        Some(1)
    );
    assert_eq!(sealed.header().member_of(f::ISSUER, RequestEpoch(2)), None);
    assert_eq!(sealed.header().member_of(f::SUBJECT, RequestEpoch(1)), None);
    assert_eq!(
        sealed.outcome(NativeInvocation::Request(first)).unwrap(),
        Some(before)
    );
    assert_eq!(
        sealed
            .outcome(NativeInvocation::Request(key(f::ISSUER, 2, 4)))
            .unwrap(),
        None
    );
    // The live core: the rows left, the window records where they went,
    // the seal's row and its own outcome are published at the next prefix.
    assert_eq!(core.native_sequence(), SessionSeq(5));
    assert_eq!(core.native_outcome(NativeInvocation::Request(first)), None);
    let after = meta(&core);
    assert_eq!((after.sealed, after.seals, after.outcomes), (3, 1, 5));
    assert_eq!(core.resident_outcomes(), 2);
    let sealed_window = window(&core, f::ISSUER);
    assert_eq!(sealed_window.sealed, RequestEpoch(2));
    assert_eq!(sealed_window.seal_of(RequestEpoch(1)), Some(1));
    assert_eq!(sealed_window.seal_of(RequestEpoch(2)), None);
    assert_eq!(
        sealed_window.ranges(),
        &[SealedRange {
            first: RequestEpoch(1),
            last: RequestEpoch(1),
            seal: 1,
        }]
    );
    let (ordinal, row) = core.native_seal(1).unwrap();
    assert_eq!(ordinal, 1);
    assert_eq!(
        row,
        SealRow {
            bundle: hash,
            bytes: bytes.len() as u64,
            through: SessionSeq(4),
            count: plan.rows() as u64,
            sealed_at: SessionSeq(5),
            first: 1,
        }
    );
    assert!(!row.is_fold(1));
    let outcome = core.native_outcome(NativeInvocation::Seal(1)).unwrap();
    assert_eq!(outcome.operation, NativeOperation::Seal);
    assert_eq!(outcome.sequence, SessionSeq(5));
    assert_eq!(outcome.changed, plan.rows() as u32);
    assert_eq!(core.native_seal_rows().unwrap(), vec![(1, row)]);
    // The exact retry of a sealed request is refused by name — never
    // executed again — and no generation awaits a seal: only the seal's own
    // outcome, closed as it is published, would go into the next one.
    assert!(matches!(
        refused(prepare(&mut core, 50, creation_in(1, 1, 1))),
        NativeError::Contract(ContractError::RequestHistoryExpired)
    ));
    let next = core.seal_plan(&[], bound(&core)).unwrap();
    assert!(next.principals.is_empty());
    assert_eq!((next.rows(), next.outcomes(), next.ordinal), (1, 1, 2));
    // The checkpoint restores it all; an owner rebuilds over it and admits
    // the open generation's work.
    let restored = restore_with(&core, &store);
    assert_eq!(restored.native_seal(1), Some((1, row)));
    assert_eq!(window(&restored, f::ISSUER), sealed_window);
    assert_eq!(restored.resident_outcomes(), 2);
    let owner =
        NativeOwner::with_record_buffers(restored, &BuiltinNativeSchemas, limits()).unwrap();
    let mut core = owner.into_committed_core().unwrap();
    assert!(matches!(
        prepare(&mut core, 60, creation_in(2, 7, 7)),
        Ok(NativePreparation::Prepared(_))
    ));
}

/// Pressure on the live window closes the least recently used open
/// generations first, by the logical time of their last request, until
/// what they hold covers the excess; a record names exactly those floors,
/// or none. A seal's index is bounded: at the bound a seal carries a fold
/// of the oldest rows into one directory row, every window's ranges follow
/// it, and the fold's directory names its members for a reader.
#[test]
fn pressure_closes_the_least_recent_generations_and_the_index_folds_at_its_bound() {
    let directory = tempfile::tempdir().unwrap();
    let store = ckpt::store(directory.path());
    // The issuer created and posted (at 10 and 20); the evaluator began two
    // admissions (at 30): four resident outcomes in two open generations.
    let mut core = f::running(&[
        (ValidationMode::Required, false),
        (ValidationMode::Required, false),
    ]);
    core.limits.outcomes = 6;
    core.limits.pending = 2;
    core.limits.seals = 2;
    // Four resident and two that may still be admitted: at the bound, not
    // past it.
    assert!(core.pressure_floors(8).unwrap().is_empty());
    // One past: the issuer's generation (last at 20) closes before the
    // evaluator's (last at 30) and covers the excess alone; three past, both
    // close, the least recent first, as many as the record may name.
    core.limits.outcomes = 5;
    assert_eq!(
        core.pressure_floors(8).unwrap(),
        vec![(f::ISSUER, RequestEpoch(2))]
    );
    assert_eq!(core.pressure_floors(0).unwrap(), vec![]);
    core.limits.outcomes = 3;
    assert_eq!(
        core.pressure_floors(8).unwrap(),
        vec![
            (f::ISSUER, RequestEpoch(2)),
            (f::EVALUATOR, RequestEpoch(2))
        ]
    );
    assert_eq!(
        core.pressure_floors(1).unwrap(),
        vec![(f::ISSUER, RequestEpoch(2))]
    );
    core.limits.outcomes = 5;
    // The record names the floors; the seal takes what they close.
    let issuer_first = key(f::ISSUER, 1, 1);
    let (first, _, first_hash, _) = seal(&mut core, &[(f::ISSUER, RequestEpoch(2))], None);
    assert_eq!(first.outcomes(), 2);
    assert_eq!(first.principals.len(), 1);
    let closed = window(&core, f::ISSUER);
    assert_eq!(
        (closed.floor, closed.sealed, closed.open),
        (RequestEpoch(2), RequestEpoch(2), 0)
    );
    assert_eq!(
        core.native_outcome(NativeInvocation::Request(issuer_first)),
        None
    );
    assert_eq!(window(&core, f::EVALUATOR).floor, RequestEpoch(1));
    assert_eq!(core.resident_outcomes(), 3);
    // The issuer's closed generation refuses its exact retries; its next
    // generation opens with its next request, and closes with the one after.
    assert!(matches!(
        refused(prepare(&mut core, 50, f::creation(1, 1, &[], None))),
        NativeError::Contract(ContractError::RequestHistoryExpired)
    ));
    f::publish(&mut core, 50, creation_in(2, 6, 6));
    f::publish(&mut core, 60, advance(f::ISSUER, 3, 7, 3));
    let reopened = window(&core, f::ISSUER);
    assert_eq!(
        (reopened.floor, reopened.sealed, reopened.open),
        (RequestEpoch(3), RequestEpoch(2), 1)
    );
    // Two past again: the evaluator's generation is the least recent now.
    let floors = core.pressure_floors(8).unwrap();
    assert_eq!(floors, vec![(f::EVALUATOR, RequestEpoch(2))]);
    let (second, _, _, _) = seal(&mut core, &floors, None);
    assert_eq!(second.principals.len(), 2);
    assert_eq!((second.ordinal, second.outcomes()), (2, 4));
    assert_eq!(
        window(&core, f::ISSUER).ranges(),
        &[
            SealedRange {
                first: RequestEpoch(1),
                last: RequestEpoch(1),
                seal: 1
            },
            SealedRange {
                first: RequestEpoch(2),
                last: RequestEpoch(2),
                seal: 2
            },
        ]
    );
    assert_eq!(
        window(&core, f::EVALUATOR).ranges(),
        &[SealedRange {
            first: RequestEpoch(1),
            last: RequestEpoch(1),
            seal: 2
        }]
    );
    assert_eq!(core.resident_outcomes(), 2);
    // At the bound (two seal rows), a third seal without a fold is refused
    // for the window that holds two ranges; with the fold of the oldest
    // two it applies: one directory row keyed by the last, the ranges
    // merged under it.
    f::publish(&mut core, 70, advance(f::ISSUER, 4, 8, 4));
    let floors = core.pressure_floors(8).unwrap();
    assert!(floors.is_empty());
    let plan = core.seal_plan(&floors, bound(&core)).unwrap();
    assert_eq!((plan.ordinal, plan.outcomes()), (3, 2));
    let quote = core.seal_quote(&plan, limits()).unwrap();
    assert!(matches!(
        core.apply_seal(SealRecord {
            floors: &floors,
            bound: SealBound {
                principals: plan.principals.len().max(1),
                rows: plan.rows(),
            },
            bundle: quote.hash,
            bytes: quote.bytes as u64,
            count: plan.rows() as u64,
            fold: None,
        }),
        Err(NativeError::Capacity("sealed ranges"))
    ));
    assert_eq!(core.native_sequence(), plan.through);
    let fold_plan = core.fold_plan(1, 2).unwrap();
    assert_eq!(fold_plan.members.len(), 2);
    assert_eq!(fold_plan.ranges.len(), 3);
    assert_eq!(fold_plan.count, (first.rows() + second.rows()) as u64);
    assert_eq!(core.fold_plan(2, 3).unwrap_err(), SealRefusal::Corrupt);
    let (third, _, _, directory_bytes) = seal(&mut core, &floors, Some((1, 2)));
    assert_eq!(third.ordinal, 3);
    // Seal one is read through the row that covers it: the fold keyed two.
    let (covering, folded) = core.native_seal(1).unwrap();
    assert_eq!(covering, 2);
    assert_eq!(core.native_seal(2), Some((2, folded)));
    assert!(folded.is_fold(2));
    assert_eq!((folded.first, folded.count), (1, fold_plan.count));
    assert_eq!(folded.through, fold_plan.through);
    assert_eq!(core.native_seal_rows().unwrap().len(), 2);
    assert_eq!(meta(&core).seals, 3);
    assert_eq!(
        window(&core, f::ISSUER).ranges(),
        &[
            SealedRange {
                first: RequestEpoch(1),
                last: RequestEpoch(2),
                seal: 2
            },
            SealedRange {
                first: RequestEpoch(3),
                last: RequestEpoch(3),
                seal: 3
            },
        ]
    );
    assert_eq!(window(&core, f::ISSUER).seal_of(RequestEpoch(1)), Some(2));
    assert_eq!(window(&core, f::ISSUER).seal_of(RequestEpoch(3)), Some(3));
    assert_eq!(
        window(&core, f::EVALUATOR).seal_of(RequestEpoch(1)),
        Some(2)
    );
    // The fold's directory names its members: a reader of a sealed
    // request follows the fold row to the member holding it.
    let directory_bytes = directory_bytes.unwrap();
    let fold = record_codec::StructuralSeal::inspect(&directory_bytes, inspection()).unwrap();
    let member = fold.header().member(1).unwrap();
    assert_eq!(member.bundle, first_hash);
    assert_eq!(member.count, first.rows() as u64);
    assert_eq!(fold.header().member(3), None);
    assert_eq!(fold.header().member_of(f::ISSUER, RequestEpoch(1)), Some(1));
    assert_eq!(fold.header().member_of(f::ISSUER, RequestEpoch(2)), Some(2));
    assert_eq!(
        fold.header().member_of(f::EVALUATOR, RequestEpoch(1)),
        Some(2)
    );
    assert_eq!(fold.header().member_of(f::ISSUER, RequestEpoch(3)), None);
    // A fold of rows already folded is refused; the checkpoint restores
    // the folded index.
    assert_eq!(core.fold_plan(1, 2).unwrap_err(), SealRefusal::Corrupt);
    let restored = restore_with(&core, &store);
    assert_eq!(
        restored.native_seal_rows().unwrap(),
        core.native_seal_rows().unwrap()
    );
    assert_eq!(window(&restored, f::ISSUER), window(&core, f::ISSUER));
}

/// A record's count must be the plan's, its bundle and length real, its
/// fold below its own ordinal: a record that says otherwise is refused
/// before anything changes.
#[test]
fn a_seal_record_that_differs_from_the_derived_plan_is_refused_unchanged() {
    let mut core = f::core();
    for claim in 1..=2u128 {
        f::publish(&mut core, 10 * claim as u64, creation_in(1, claim, claim));
    }
    f::publish(&mut core, 30, advance(f::ISSUER, 2, 3, 2));
    let plan = core.seal_plan(&[], bound(&core)).unwrap();
    let quote = core.seal_quote(&plan, limits()).unwrap();
    let record = |count: u64, bundle: ContentHash, bytes: u64, fold: Option<Fold>| SealRecord {
        floors: &[],
        bound: SealBound {
            principals: 1,
            rows: plan.rows(),
        },
        bundle,
        bytes,
        count,
        fold,
    };
    let sequence = core.native_sequence();
    for bad in [
        record(plan.rows() as u64 + 1, quote.hash, quote.bytes as u64, None),
        record(
            plan.rows() as u64,
            ContentHash([0; 32]),
            quote.bytes as u64,
            None,
        ),
        record(plan.rows() as u64, quote.hash, 0, None),
        record(
            plan.rows() as u64,
            quote.hash,
            quote.bytes as u64,
            Some(Fold {
                first: 1,
                last: 1,
                bundle: ContentHash([5; 32]),
                bytes: 1,
            }),
        ),
    ] {
        assert!(matches!(
            core.apply_seal(bad),
            Err(NativeError::Contract(ContractError::InvalidManifest))
        ));
        assert_eq!(core.native_sequence(), sequence);
        assert_eq!(meta(&core).seals, 0);
    }
    assert_eq!(
        core.apply_seal(record(
            plan.rows() as u64,
            quote.hash,
            quote.bytes as u64,
            None
        ))
        .unwrap(),
        plan.rows()
    );
}
