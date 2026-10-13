//! Retirement in the owner's pipeline (doc 31 M2): a batch prepared between writes publishes in
//! their order, its record carries families rather than rows, and a follower derives the same
//! families from its own rows and ends in the leader's state, or refuses a record its rows
//! cannot match.
use super::record_codec::recovery::tests as ckpt;
use super::record_codec::{InspectionLimits, RETIRE_MAGIC, RetireRecord, StructuralRecord};
use super::report_tests as f;
use super::retirement::RetirementRefusal;
use super::retirement_batch::*;
use super::*;
use focal_evidence::{BuiltinNativeSchemas, ContentStore};

fn id(value: u128) -> ClaimId {
    ClaimId::from_u128(value)
}
fn cancel(core: &mut Core<NativeState>, request: u128, claim: u128, time: u64) {
    let expected = core.native_claim(id(claim)).unwrap().binding();
    f::publish(
        core,
        time,
        NativeInput {
            request: f::request(f::ISSUER, request),
            command: NativeCommand::Cancel { expected },
        },
    );
}
fn release(core: &mut Core<NativeState>, request: u128, claim: u128, time: u64) {
    let expected = core.native_claim(id(claim)).unwrap().binding();
    f::publish(
        core,
        time,
        NativeInput {
            request: f::request(f::ISSUER, request),
            command: NativeCommand::ReleaseScope { expected },
        },
    );
}
/// Create, cancel and release one claim: a family of one.
fn finished(core: &mut Core<NativeState>, claim: u128, request: u128, time: u64) {
    f::publish(core, time, f::creation(request, claim, &[], None));
    cancel(core, request + 1, claim, time + 10);
    release(core, request + 2, claim, time + 20);
}
fn encoding() -> record_codec::EncodingLimits {
    record_codec::EncodingLimits {
        bytes: 32 << 20,
        visits: 100_000_000,
        rows: 100_000,
    }
}
fn inspection(bytes: &[u8]) -> InspectionLimits {
    InspectionLimits {
        bytes: bytes.len(),
        visits: 1_000_000_000,
        rows: 100_000,
        row_bytes: 32 << 20,
    }
}
fn restored(original: &Core<NativeState>, range: u128, store: &ContentStore) -> Core<NativeState> {
    let bytes = ckpt::encode(original);
    record_codec::recovery::restore(
        &ckpt::inspect(&bytes),
        RangeId(range),
        ckpt::limits(original.limits),
        ckpt::budget(),
        store,
        &BuiltinNativeSchemas,
    )
    .unwrap()
}
/// What the archive agent would offer for `root`: its family's digest and a bundle.
fn offer(core: &Core<NativeState>, root: u128) -> RetireOffer {
    let family = core.retirement_family(id(root)).unwrap();
    RetireOffer {
        root: id(root),
        family: family_digest(core.state.ledger, &family).unwrap(),
        bundle: ContentHash([u8::try_from(root % 250).unwrap() + 1; 32]),
        bytes: 1000 + u64::try_from(root).unwrap(),
    }
}
fn owner(core: Core<NativeState>) -> NativeOwner {
    NativeOwner::with_record_buffers(core, &BuiltinNativeSchemas, encoding()).unwrap()
}
fn stage(owner: &mut NativeOwner, time: u64, input: NativeInput) -> NativeCandidate {
    match owner
        .prepare(f::context(input.request.principal, time), input, None)
        .unwrap()
    {
        NativeStaging::Prepared { candidate, .. } => candidate,
        NativeStaging::Existing { .. } => panic!("expected a fresh candidate"),
    }
}
fn record(owner: &mut NativeOwner, candidate: NativeCandidate) -> Vec<u8> {
    owner
        .encode_candidate(candidate, encoding())
        .unwrap()
        .bytes()
        .to_vec()
}
/// Replay a native record into a follower's core and publish it.
fn replay_into(
    core: &mut Core<NativeState>,
    bytes: &[u8],
    original: RangeId,
    store: &ContentStore,
) {
    let prepared = record_codec::replay::prepare(
        core,
        &StructuralRecord::inspect(bytes, inspection(bytes)).unwrap(),
        original,
        ckpt::limits(core.limits),
        store,
        &BuiltinNativeSchemas,
    )
    .unwrap();
    core.publish_native(prepared).unwrap();
}
/// The base both replicas start from: two finished families and an open claim.
fn base() -> Core<NativeState> {
    let mut core = f::core();
    finished(&mut core, 1, 1, 10);
    finished(&mut core, 2, 10, 40);
    f::publish(&mut core, 70, f::creation(20, 20, &[], None));
    core
}

/// A batch prepared between two writes takes its place in their order: the first write, the
/// retirement and the second publish one after another at consecutive sequences, nothing
/// drained or rebuilt between them. Its record carries the two families by root and bundle,
/// a few dozen bytes each; a follower replays the writes, derives the families from its own
/// rows at the record's place, and ends in the leader's state, row for row.
#[test]
fn a_retirement_is_ordered_with_the_writes_around_it_and_a_follower_derives_it_alike() {
    let directory = tempfile::tempdir().unwrap();
    let store = ckpt::store(directory.path());
    let base = base();
    let offers = [offer(&base, 1), offer(&base, 2)];
    let leader_core = restored(&base, 801, &store);
    let range = leader_core.state.rows.id();
    let mut follower = restored(&base, 802, &store);
    let mut owner = owner(leader_core);
    let first = stage(&mut owner, 80, f::creation(30, 30, &[], None));
    let (batch, staged) = owner.prepare_retirement(&offers).unwrap();
    assert_eq!(batch.roots().collect::<Vec<_>>(), [id(1), id(2)]);
    assert!(batch.deferred.is_empty() && batch.refused.is_empty());
    let Some(NativeStaging::Prepared {
        candidate: retirement,
        outcome,
    }) = staged
    else {
        panic!("a retirement candidate");
    };
    assert_eq!(outcome.operation, NativeOperation::Retire);
    assert_eq!(outcome.invocation, NativeInvocation::Retirement(id(1)));
    let second = stage(&mut owner, 90, f::creation(40, 40, &[], None));
    assert_eq!(owner.pending_len(), 3, "the writes go on around it");
    let first_record = record(&mut owner, first);
    let retirement_record = record(&mut owner, retirement);
    let second_record = record(&mut owner, second);
    assert!(retirement_record.starts_with(&RETIRE_MAGIC));
    assert!(
        retirement_record.len() < 512,
        "families, not rows: {} bytes",
        retirement_record.len()
    );
    let inspected =
        RetireRecord::inspect(&retirement_record, inspection(&retirement_record), 64).unwrap();
    assert_eq!(inspected.header().outcome, outcome);
    assert_eq!(
        inspected.retired(),
        [
            RetiredBundle {
                root: id(1),
                bundle: offers[0].bundle,
                bytes: offers[0].bytes
            },
            RetiredBundle {
                root: id(2),
                bundle: offers[1].bundle,
                bytes: offers[1].bytes
            },
        ]
    );
    let before = owner.committed_core().native_sequence();
    for candidate in [first, retirement, second] {
        owner.publish_after_durable(candidate).unwrap();
    }
    let leader = owner.committed_core();
    assert_eq!(leader.native_sequence().0, before.0 + 3);
    for claim in [1, 2] {
        assert!(leader.native_claim(id(claim)).is_none());
        let retired = leader.native_retired(id(claim)).unwrap();
        assert_eq!(retired.retired_at, outcome.sequence);
        assert_eq!(
            retired.bundle,
            offers[usize::try_from(claim - 1).unwrap()].bundle
        );
    }
    assert!(leader.native_claim(id(30)).is_some() && leader.native_claim(id(40)).is_some());
    assert!(leader.state.events.complete(), "its events unfiled with it");
    // The follower: the writes replayed, the retirement derived.
    replay_into(&mut follower, &first_record, range, &store);
    assert_eq!(
        follower.apply_retirement_record(&inspected).unwrap(),
        outcome
    );
    replay_into(&mut follower, &second_record, range, &store);
    ckpt::compare(owner.committed_core(), &follower);
    assert!(follower.state.events.complete());
}

/// A family a pending candidate touches waits: here the families an earlier retirement, not
/// yet published, takes are offered again and deferred. A family that cannot leave says why,
/// and one whose bundle holds another family than the one derived now is refused; with nothing
/// to retire no candidate is made.
#[test]
fn a_touched_family_waits_and_a_refused_one_says_why() {
    let directory = tempfile::tempdir().unwrap();
    let store = ckpt::store(directory.path());
    let base = base();
    let mut owner = owner(restored(&base, 803, &store));
    let first = [offer(&base, 1)];
    let (batch, staged) = owner.prepare_retirement(&first).unwrap();
    assert_eq!(batch.families.len(), 1);
    let candidate = match staged {
        Some(NativeStaging::Prepared { candidate, .. }) => candidate,
        other => panic!("{other:?}"),
    };
    let mut stale = offer(&base, 2);
    stale.family = ContentHash([9; 32]);
    let open = RetireOffer {
        root: id(20),
        ..offer(&base, 1)
    };
    let (batch, staged) = owner
        .prepare_retirement(&[offer(&base, 1), stale, open])
        .unwrap();
    assert!(staged.is_none(), "nothing to retire, no candidate");
    assert_eq!(batch.deferred, [id(1)], "taken by the pending retirement");
    assert_eq!(
        batch.refused,
        [
            (id(2), RetireRefusal::Bundle),
            (
                id(20),
                RetireRefusal::Family(RetirementRefusal::NotTerminal(id(20)))
            ),
        ]
    );
    // Published, the family is gone; the other retires on its own offer.
    owner.publish_after_durable(candidate).unwrap();
    let (batch, staged) = owner.prepare_retirement(&[offer(&base, 2)]).unwrap();
    assert_eq!(batch.roots().collect::<Vec<_>>(), [id(2)]);
    assert!(staged.is_some());
}

/// A follower whose rows cannot derive the record's families alike refuses the record as a
/// divergence and keeps its state: here its second family was never released.
#[test]
fn a_follower_whose_rows_differ_refuses_the_record_and_keeps_its_state() {
    let directory = tempfile::tempdir().unwrap();
    let store = ckpt::store(directory.path());
    let base = base();
    let offers = [offer(&base, 1), offer(&base, 2)];
    let mut owner = owner(restored(&base, 804, &store));
    let (_, staged) = owner.prepare_retirement(&offers).unwrap();
    let Some(NativeStaging::Prepared { candidate, .. }) = staged else {
        panic!("a retirement candidate");
    };
    let bytes = record(&mut owner, candidate);
    let inspected = RetireRecord::inspect(&bytes, inspection(&bytes), 64).unwrap();
    // A follower that never saw family 2's release: a divergent history.
    let mut divergent = f::core();
    finished(&mut divergent, 1, 1, 10);
    f::publish(&mut divergent, 40, f::creation(10, 2, &[], None));
    cancel(&mut divergent, 11, 2, 50);
    f::publish(&mut divergent, 60, f::creation(12, 21, &[], None));
    f::publish(&mut divergent, 70, f::creation(20, 20, &[], None));
    assert_eq!(divergent.native_sequence(), base.native_sequence());
    let sequence = divergent.native_sequence();
    assert!(matches!(
        divergent.apply_retirement_record(&inspected),
        Err(NativeError::Contract(ContractError::InvalidManifest))
    ));
    assert_eq!(divergent.native_sequence(), sequence);
    assert!(divergent.native_claim(id(1)).is_some(), "nothing retired");
    // A record altered on its way is refused by its digest.
    let mut tampered = bytes.clone();
    let at = tampered.len() - 40;
    tampered[at] ^= 1;
    assert!(RetireRecord::inspect(&tampered, inspection(&tampered), 64).is_err());
}

/// Every key digests whole, the widest of each kind included: a result's, a verdict's, a due
/// timer's and a deadline outcome's, each naming an evaluation of a work artifact. A family
/// holding a key past what the digest writes could never be offered: a family of work results
/// once could not.
#[test]
fn the_widest_keys_digest_whole() {
    let work = EvaluationKey {
        claim: id(1),
        validation: ValidationId::from_u128(u128::MAX),
        target: EvaluationTarget::Work {
            response: TestamentId::from_u128(u128::MAX),
            slot: u32::MAX,
            artifact: ArtifactId::from_u128(u128::MAX),
        },
        generation: u64::MAX,
    };
    let result = NativeResultKey {
        evaluation: work,
        revision: focal_model::ObjectRevision(u64::MAX),
    };
    let keys = [
        Key::Accepted(result),
        Key::MissingResult(result),
        Key::DeliveryResult(result),
        Key::ByVerdict(u16::MAX, result),
        Key::Evaluation(work),
        Key::DueTimer(u64::MAX, TimerTarget::Evaluation(work)),
        Key::ByScope(u16::MAX, ContentHash([0xff; 32]), id(u128::MAX)),
        Key::Outcome(NativeInvocation::EvaluationDeadline(NativeDeadlineKey {
            evaluation: work,
            timer: TimerId::from_u128(u128::MAX),
            generation: u64::MAX,
        })),
    ];
    for key in keys {
        let mut hasher = blake3::Hasher::new();
        assert!(
            record_codec::hash_key(&mut hasher, key).is_ok(),
            "{key:?} does not digest"
        );
    }
}
