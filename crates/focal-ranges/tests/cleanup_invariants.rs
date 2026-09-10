#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
use focal_memory::{BudgetKind, BudgetLane, MemoryBudget};
use focal_model::{
    ContentHash, LedgerId, RaftIndex, RaftTerm, RouteEpoch, SessionId, SessionSeq, TenantId,
};
use focal_ranges::*;

fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(1),
        session: SessionId::from_u128(2),
    }
}
fn budget() -> MemoryBudget {
    MemoryBudget::new(32 * 1024 * 1024, 4 * 1024 * 1024).unwrap()
}
fn key(value: u8) -> StorageKey {
    StorageKey::bucket([value; 16], 0, 0)
}
fn descriptor(id: u128, start: Option<StorageKey>, end: Option<StorageKey>) -> RangeDescriptor {
    RangeDescriptor {
        id: RangeId::from_u128(id),
        generation: 1,
        span: KeySpan { start, end },
        meta: Placement::replica(ReplicaId {
            node: 1,
            generation: 1,
        }),
    }
}
fn map() -> RangeMap {
    RangeMap::new(
        ledger(),
        RouteEpoch(1),
        vec![descriptor(1, None, None)],
        RangeLimits::default(),
    )
    .unwrap()
}
fn intent() -> RangeIntent {
    RangeIntent {
        ledger: ledger(),
        operation: TransferId::from_u128(1),
        old_epoch: RouteEpoch(1),
        sources: [RangeId::from_u128(1)].into_iter().collect(),
        replacements: vec![descriptor(2, None, None)],
        seed: SessionSeq(0),
    }
}
// This fixture authorizes only a marked local test receipt. Production hosts
// must verify session consensus authority and custody; a nonzero hash is not it.
struct Verified;
impl RangeVerifier for Verified {
    fn commit(&self, proof: &CommitProof) -> Result<(), RangeError> {
        if proof.attestation == ContentHash([7; 32]) {
            Ok(())
        } else {
            Err(RangeError::Unverified)
        }
    }
    fn batch(&self, _: &RangeBatch, proof: &CommitProof) -> Result<(), RangeError> {
        self.commit(proof)
    }
    fn source_seal(&self, _: &SourceSealProof) -> Result<(), RangeError> {
        Err(RangeError::Unverified)
    }
    fn destination_ready(&self, _: &DestinationReady) -> Result<(), RangeError> {
        Err(RangeError::Unverified)
    }
    fn recovery(&self, _: &RecoveryProof) -> Result<(), RangeError> {
        Err(RangeError::Unverified)
    }
    fn progress(&self, proof: &RangeProgress) -> Result<(), RangeError> {
        if proof.attestation == ContentHash([7; 32]) {
            Ok(())
        } else {
            Err(RangeError::Unverified)
        }
    }
    fn read(&self, proof: &ReadAvailability) -> Result<(), RangeError> {
        if proof.attestation == ContentHash([7; 32]) {
            Ok(())
        } else {
            Err(RangeError::Unverified)
        }
    }
}
fn commit(sequence: u64, term: u64, command: ContentHash) -> CommitProof {
    commit_at(sequence, sequence, term, command)
}
/// A control commit at `ordinal` applied at native prefix `sequence`.
fn commit_at(ordinal: u64, sequence: u64, term: u64, command: ContentHash) -> CommitProof {
    CommitProof {
        ledger: ledger(),
        ordinal,
        sequence: SessionSeq(sequence),
        index: RaftIndex(sequence),
        term: RaftTerm(term),
        command,
        attestation: ContentHash([7; 32]),
    }
}
fn progress(sequence: u64, term: u64) -> RangeProgress {
    RangeProgress {
        ledger: ledger(),
        epoch: RouteEpoch(1),
        range: RangeId::from_u128(1),
        range_generation: 1,
        replica: ReplicaId {
            node: 1,
            generation: 1,
        },
        through: SessionSeq(sequence),
        term: RaftTerm(term),
        root: ContentHash([9; 32]),
        attestation: ContentHash([7; 32]),
    }
}

#[test]
fn map_boundaries_split_generations_and_epoch_exhaustion_are_checked() {
    let limits = RangeLimits::default();
    let split = map()
        .replace(
            &[RangeId::from_u128(1)].into_iter().collect(),
            vec![
                descriptor(2, None, Some(key(8))),
                descriptor(3, Some(key(8)), None),
            ],
            limits,
        )
        .unwrap();
    assert_eq!(split.route(key(7)).unwrap().id, RangeId::from_u128(2));
    assert_eq!(split.route(key(8)).unwrap().id, RangeId::from_u128(3));
    assert_eq!(
        RangeMap::new(
            ledger(),
            RouteEpoch(1),
            vec![
                descriptor(2, None, Some(key(8))),
                descriptor(3, Some(key(9)), None)
            ],
            limits
        ),
        Err(RangeError::Gap)
    );
    assert_eq!(
        RangeMap::new(
            ledger(),
            RouteEpoch(1),
            vec![
                descriptor(2, None, Some(key(9))),
                descriptor(3, Some(key(8)), None)
            ],
            limits
        ),
        Err(RangeError::Overlap)
    );
    let sources = [RangeId::from_u128(1)].into_iter().collect();
    assert_eq!(
        map().replace(&sources, vec![descriptor(1, None, None)], limits),
        Err(RangeError::Generation)
    );
    let exhausted = RangeMap::new(
        ledger(),
        RouteEpoch(u64::MAX),
        map().ranges().to_vec(),
        limits,
    )
    .unwrap();
    assert_eq!(
        exhausted.replace(&sources, vec![descriptor(2, None, None)], limits),
        Err(RangeError::Overflow)
    );
}

#[test]
fn prepared_metadata_is_owned_fenced_and_releases_budget_on_rejection() {
    let memory = budget();
    let limits = RangeLimits::default();
    let mut owner = RangeCoordinator::new(
        map(),
        ControllerIncarnation::from_u128(1),
        limits,
        memory.clone(),
    )
    .unwrap();
    let mut foreign = RangeCoordinator::new(
        map(),
        ControllerIncarnation::from_u128(1),
        limits,
        memory.clone(),
    )
    .unwrap();
    let initial = memory.stats().used;
    let prepared = owner
        .prepare(1, SessionSeq(1), RangeOperation::Begin(intent()), &Verified)
        .unwrap();
    assert!(memory.stats().used > initial);
    let proof = commit(1, 1, prepared.hash());
    assert_eq!(
        foreign.publish(prepared, &proof, &Verified),
        Err(RangeError::StalePreparation)
    );
    assert_eq!(memory.stats().used, initial);
    assert!(owner.pending().is_none());
    let prepared = owner
        .prepare(1, SessionSeq(1), RangeOperation::Begin(intent()), &Verified)
        .unwrap();
    let remaining = memory.stats().limit - memory.stats().used;
    let pressure = memory
        .reserve(BudgetKind::Control, BudgetLane::Completion, remaining)
        .unwrap();
    owner.publish(prepared, &proof, &Verified).unwrap();
    assert_eq!(owner.pending().unwrap().intent, intent());
    drop(pressure);
    let published = memory.stats().used;
    let retry = owner
        .prepare(1, SessionSeq(1), RangeOperation::Begin(intent()), &Verified)
        .unwrap();
    owner.publish(retry, &proof, &Verified).unwrap();
    assert_eq!(memory.stats().used, published);
    drop(owner);
    drop(foreign);
    assert_eq!(memory.stats().used, 0);
}

#[test]
fn checkpoint_publication_floor_survives_new_term_and_prepared_state() {
    let memory = budget();
    let mut owner = RangeCoordinator::new(
        map(),
        ControllerIncarnation::from_u128(1),
        RangeLimits::default(),
        memory.clone(),
    )
    .unwrap();
    let prepared = owner
        .prepare(
            1,
            SessionSeq(11),
            RangeOperation::Begin(intent()),
            &Verified,
        )
        .unwrap();
    owner
        .observe_committed(&commit(10, 1, ContentHash([1; 32])), &Verified)
        .unwrap();
    owner.observe_progress(progress(10, 1), &Verified).unwrap();
    assert_eq!(owner.published(), Some(SessionSeq(10)));
    let proof = commit_at(1, 11, 1, prepared.hash());
    owner.publish(prepared, &proof, &Verified).unwrap();
    assert_eq!(owner.checkpoint().published, SessionSeq(10));
    owner
        .observe_committed(&commit(12, 2, ContentHash([2; 32])), &Verified)
        .unwrap();
    assert_eq!(owner.published(), None);
    owner.observe_progress(progress(9, 2), &Verified).unwrap();
    assert_eq!(owner.published(), None);
    owner.observe_progress(progress(12, 2), &Verified).unwrap();
    let state = owner.checkpoint().clone();
    assert_eq!(state.published, SessionSeq(12));
    let mut restored = RangeCoordinator::restore(
        state,
        ControllerIncarnation::from_u128(2),
        RangeLimits::default(),
        memory,
    )
    .unwrap();
    restored
        .observe_committed(&commit(12, 3, ContentHash([3; 32])), &Verified)
        .unwrap();
    restored
        .observe_progress(progress(11, 3), &Verified)
        .unwrap();
    assert_eq!(restored.published(), None);
    restored
        .observe_progress(progress(12, 3), &Verified)
        .unwrap();
    assert_eq!(restored.published(), Some(SessionSeq(12)));
}

#[test]
fn staged_block_retries_checksums_and_failed_reservations_preserve_state() {
    let memory = budget();
    let limits = RangeLimits::default();
    let block = RangeBlock {
        index: 0,
        rows: vec![DataRow {
            key: key(1),
            value: vec![4; 128],
        }],
    };
    let manifest = SeedManifest {
        ledger: ledger(),
        operation: TransferId::from_u128(1),
        source_epoch: RouteEpoch(1),
        target_epoch: RouteEpoch(2),
        destination: descriptor(2, None, None),
        prefix: SessionSeq(0),
        blocks: vec![BlockDescriptor {
            index: 0,
            rows: 1,
            bytes: postcard::experimental::serialized_size(&block).unwrap(),
            first: key(1),
            last: key(1),
            hash: block.hash().unwrap(),
        }],
    };
    let mut stager = RangeStager::new(manifest, limits, memory.clone()).unwrap();
    let before = memory.stats().used;
    let pressure = memory
        .reserve(
            BudgetKind::Control,
            BudgetLane::Completion,
            memory.stats().limit - before,
        )
        .unwrap();
    assert!(matches!(
        stager.accept(block.clone()),
        Err(RangeError::Memory(_))
    ));
    assert_eq!(stager.accepted_blocks(), 0);
    drop(pressure);
    assert_eq!(memory.stats().used, before);
    let mut corrupt = block.clone();
    corrupt.rows[0].value[0] = 8;
    assert_eq!(stager.accept(corrupt), Err(RangeError::Checksum));
    stager.accept(block.clone()).unwrap();
    let held = memory.stats().used;
    stager.accept(block).unwrap();
    assert_eq!(memory.stats().used, held);
    let checkpoint = stager.checkpoint().unwrap();
    let restored = RangeStager::restore(
        checkpoint.bytes(),
        checkpoint.hash(),
        limits,
        memory.clone(),
    )
    .unwrap();
    let replica = restored.install(42).unwrap();
    assert_eq!(replica.get(key(1)), Some(vec![4; 128].as_slice()));
    assert!(matches!(replica.role(), ReplicaRole::Staging { .. }));
    drop(replica);
    drop(checkpoint);
    drop(stager);
    assert_eq!(memory.stats().used, 0);
}

#[test]
fn read_plan_owns_its_charge_and_cursor_scope_expires() {
    let memory = budget();
    let mut owner = RangeCoordinator::new(
        map(),
        ControllerIncarnation::from_u128(1),
        RangeLimits::default(),
        memory.clone(),
    )
    .unwrap();
    owner
        .observe_committed(&commit(1, 1, ContentHash([1; 32])), &Verified)
        .unwrap();
    owner.observe_progress(progress(1, 1), &Verified).unwrap();
    let availability = ReadAvailability {
        ledger: ledger(),
        epoch: RouteEpoch(1),
        range: RangeId::from_u128(1),
        range_generation: 1,
        replica: ReplicaId {
            node: 1,
            generation: 1,
        },
        prefix: SessionSeq(1),
        lease: 1,
        expires_at: 10,
        root: ContentHash([9; 32]),
        attestation: ContentHash([7; 32]),
    };
    let cursor = owner
        .pin(
            PinRequest {
                prefix: SessionSeq(1),
                query: QueryId::from_u128(1),
                span: KeySpan::all(),
                now: 0,
                ttl: 10,
            },
            &[availability],
            &Verified,
        )
        .unwrap();
    let mut altered = cursor.clone();
    altered.query = QueryId::from_u128(2);
    assert!(matches!(
        owner.read_plan(&altered, 0, &Verified),
        Err(RangeError::Conflict)
    ));
    let plan = owner.read_plan(&cursor, 1, &Verified).unwrap();
    assert_eq!(plan.fragments.len(), 1);
    assert!(matches!(
        owner.read_plan(&cursor, 10, &Verified),
        Err(RangeError::Expired)
    ));
    drop(owner);
    assert!(memory.stats().used > 0);
    drop(plan);
    assert_eq!(memory.stats().used, 0);
}

#[test]
fn activation_proof_digest_binds_the_exact_transfer_intent() {
    let certificate = ActivationCertificate {
        intent: intent(),
        map: map(),
        barrier: commit(1, 1, ContentHash([1; 32])),
        snapshots: Default::default(),
        sources: Default::default(),
        destinations: Default::default(),
        unchanged: vec![],
        commit: commit(2, 1, ContentHash([2; 32])),
    };
    let hash = certificate.proof_digest().unwrap();
    let mut changed = certificate.clone();
    changed.intent.seed = SessionSeq(1);
    assert_ne!(changed.proof_digest().unwrap(), hash);
    changed = certificate;
    changed.intent.sources.insert(RangeId::from_u128(9));
    assert_ne!(changed.proof_digest().unwrap(), hash);
}
