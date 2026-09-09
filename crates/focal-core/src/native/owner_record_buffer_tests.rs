//! Record buffers and their permits live exactly as long as their candidate:
//! reserved at admission, encoded on demand, released once on publication or
//! discard. Read leases keep only their own charge once the owner is gone.
use super::report_tests::{ISSUER, context, core, creation, post};
use super::*;
use focal_evidence::BuiltinNativeSchemas;
use focal_memory::{ALLOCATOR_OVERHEAD, MemoryBudget};
use focal_model::{ClaimId, ClaimStatus};

fn encoding() -> record_codec::EncodingLimits {
    record_codec::EncodingLimits {
        bytes: 32 << 20,
        visits: 100_000_000,
        rows: 100_000,
    }
}
fn owner(core: Core<NativeState>) -> NativeOwner {
    NativeOwner::with_record_buffers(core, &BuiltinNativeSchemas, encoding()).unwrap()
}
fn stage(owner: &mut NativeOwner, time: u64, input: NativeInput) -> NativeCandidate {
    match owner
        .prepare(context(input.request.principal, time), input, None)
        .unwrap()
    {
        NativeStaging::Prepared { candidate, .. } => candidate,
        NativeStaging::Existing { .. } => panic!("expected a fresh candidate"),
    }
}
fn budget(owner: &NativeOwner) -> MemoryBudget {
    owner.budget_for_test().clone()
}

#[test]
fn encode_refusal_keeps_the_reserved_permit_and_later_encodes_yield_identical_bytes() {
    let mut owner = owner(core());
    let budget = budget(&owner);
    let idle = budget.stats().used;
    let candidate = stage(&mut owner, 10, creation(1, 1, &[], None));
    let reserved = budget.stats().used;
    assert!(
        reserved > idle,
        "admission charges the future record buffer"
    );
    let tight = record_codec::EncodingLimits {
        bytes: 1,
        ..encoding()
    };
    assert!(matches!(
        owner.encode_candidate(candidate, tight),
        Err(NativeOwnerError::Record(record_codec::CodecError::Capacity))
    ));
    assert_eq!(budget.stats().used, reserved, "refusal keeps the permit");
    assert_eq!(owner.pending_len(), 1);
    let (first_bytes, first_hash, first_address, charged) = {
        let record = owner.encode_candidate(candidate, encoding()).unwrap();
        (
            record.bytes().to_vec(),
            record.hash(),
            record.bytes().as_ptr(),
            record.charged_bytes(),
        )
    };
    assert_eq!(charged, first_bytes.len() + ALLOCATOR_OVERHEAD);
    assert_eq!(
        budget.stats().used,
        reserved,
        "encoding fills the preheld permit without a second charge"
    );
    let record = owner.encode_candidate(candidate, encoding()).unwrap();
    assert_eq!(record.bytes(), first_bytes.as_slice());
    assert_eq!(record.hash(), first_hash);
    assert_eq!(
        record.bytes().as_ptr(),
        first_address,
        "retries reuse the buffer"
    );
    // A cap below the exact quote refuses without touching the encoded record.
    let below = record_codec::EncodingLimits {
        visits: record.quote().visits - 1,
        ..encoding()
    };
    assert!(matches!(
        owner.encode_candidate(candidate, below),
        Err(NativeOwnerError::Record(record_codec::CodecError::Capacity))
    ));
    let record = owner.encode_candidate(candidate, encoding()).unwrap();
    assert_eq!(record.bytes().as_ptr(), first_address);
    assert_eq!(budget.stats().used, reserved);
}

#[test]
fn publication_and_discard_release_each_record_charge_exactly_once() {
    let mut owner = owner(core());
    let budget = budget(&owner);
    let idle = budget.stats().used;
    let first = stage(&mut owner, 10, creation(1, 1, &[], None));
    let second = stage(&mut owner, 20, creation(2, 2, &[], None));
    let first_charge = owner
        .encode_candidate(first, encoding())
        .unwrap()
        .charged_bytes();
    let second_charge = owner
        .encode_candidate(second, encoding())
        .unwrap()
        .charged_bytes();
    let full = budget.stats().used;
    owner.publish_after_durable(first).unwrap();
    let after_publish = budget.stats().used;
    assert!(
        after_publish + first_charge <= full,
        "publication returns the whole record charge ({full} -> {after_publish}, record {first_charge})"
    );
    assert_eq!(owner.discard_from(second).unwrap(), 1);
    let after_discard = budget.stats().used;
    assert!(
        after_discard + second_charge <= after_publish,
        "discard returns the whole record charge ({after_publish} -> {after_discard}, record {second_charge})"
    );
    assert!(matches!(
        owner.discard_from(second),
        Err(NativeOwnerError::UnknownCandidate)
    ));
    assert_eq!(
        budget.stats().used,
        after_discard,
        "a second discard refunds nothing"
    );
    assert!(matches!(
        owner.encode_candidate(second, encoding()),
        Err(NativeOwnerError::UnknownCandidate)
    ));
    assert_eq!(
        owner
            .committed()
            .claim(ClaimId::from_u128(1))
            .unwrap()
            .status(),
        ClaimStatus::Generated
    );
    assert!(owner.committed().claim(ClaimId::from_u128(2)).is_none());
    assert!(after_discard >= idle);
    drop(owner);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn extraction_refuses_while_candidates_are_pending_and_returns_the_identical_owner() {
    let mut owner = owner(core());
    let budget = budget(&owner);
    let created = stage(&mut owner, 10, creation(1, 1, &[], None));
    let hash = owner.encode_candidate(created, encoding()).unwrap().hash();
    let before = budget.stats();
    let refused = owner.into_committed_core().unwrap_err();
    assert!(matches!(refused.error, NativeOwnerError::PendingCandidates));
    let mut owner = refused.owner;
    assert_eq!(
        budget.stats(),
        before,
        "refusal allocates and releases nothing"
    );
    assert_eq!(owner.pending_len(), 1);
    assert_eq!(owner.oldest(), Some(created));
    assert_eq!(
        owner.encode_candidate(created, encoding()).unwrap().hash(),
        hash,
        "the retained record is the same record"
    );
    let outcome = owner.publish_after_durable(created).unwrap();
    let core = owner.into_committed_core().unwrap();
    assert_eq!(core.native_outcome(outcome.invocation), Some(outcome));
    drop(core);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn a_read_lease_keeps_only_its_own_charge_after_the_owner_is_gone() {
    let mut owner = owner(core());
    let budget = budget(&owner);
    let created = stage(&mut owner, 10, creation(1, 1, &[], None));
    owner.publish_after_durable(created).unwrap();
    let posted = stage(&mut owner, 20, post(2, super::report_tests::binding(1)));
    let read = owner.pin(0, 100).unwrap();
    assert_eq!(read.sequence(), SessionSeq(1));
    assert_eq!(
        read.with_claim(ClaimId::from_u128(1), 50, |row| row.status())
            .unwrap(),
        Some(ClaimStatus::Generated),
        "a lease observes the committed prefix, never the pending one"
    );
    owner.publish_after_durable(posted).unwrap();
    let core = owner.into_committed_core().unwrap();
    assert!(
        read.with_claim(ClaimId::from_u128(1), 50, |row| row.status())
            .is_ok(),
        "the registry outlives the owner while the core lives"
    );
    drop(core);
    let remaining = budget.stats().used;
    assert!(
        remaining > 0,
        "the lease's own metadata charge stays until it drops"
    );
    assert!(
        read.with_claim(ClaimId::from_u128(1), 50, |row| row.status())
            .is_err(),
        "no page survives its owner; the lease reports the loss instead of reading freed rows"
    );
    drop(read);
    assert_eq!(budget.stats().used, 0);
    let _ = ISSUER;
}
