//! Movement records (25 §6): each step of a range transfer is one session
//! decision the authority proposes and every replica applies between native
//! records, running the range crate's coordinator against the committed
//! state under a commit proof minted from the entry. Proofs are attested
//! from the session genesis in this step; node-signed proofs arrive with the
//! materializer host. The coordinator state travels in the session
//! checkpoint, so a restart or a snapshot resumes from the committed step.
use super::*;
use focal_model::{RaftIndex, RaftTerm, RouteEpoch};
use focal_ranges::{
    CommitProof, ControllerIncarnation, DestinationReady, KeySpan, Placement, RangeBatch,
    RangeCheckpoint, RangeCoordinator, RangeDescriptor, RangeError, RangeLimits, RangeMap,
    RangeOperation, RangeProgress, RangeVerifier, ReadAvailability, RecoveryProof, SourceSealProof,
    StorageKey, TransferState,
};
use serde::Serialize;
use std::collections::BTreeSet;

pub const MAGIC: [u8; 8] = *b"FOCALRM1";
pub const VERSION: u16 = 1;
/// The largest encoded movement record: magic, version, ledger, ordinal,
/// the operation's length and bytes, and the digest.
pub const MAX_RECORD_BYTES: usize = 64 * 1024;
const HEAD: usize = 8 + 2 + 32 + 8 + 4;
const HASH_DOMAIN: &str = "focal.native.session.movement-record.v1";
const COMMIT_DOMAIN: &str = "focal.range-commit.v1";
const SEAL_DOMAIN: &str = "focal.range-source-seal.v1";
const READY_DOMAIN: &str = "focal.range-destination-ready.v1";
const PROGRESS_DOMAIN: &str = "focal.range-progress.v1";
const READ_DOMAIN: &str = "focal.range-read.v1";
const RECOVERY_DOMAIN: &str = "focal.range-recovery.v1";

/// One movement step: the ordinal it expects to be (one more than the last
/// applied record's) and the coordinator operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MovementRecord {
    pub ledger: LedgerId,
    pub ordinal: u64,
    pub operation: RangeOperation,
}

impl MovementRecord {
    pub fn encode(&self) -> Result<Vec<u8>, NativeSessionError> {
        let body =
            postcard::to_allocvec(&self.operation).map_err(|_| NativeSessionError::Corrupt)?;
        let length = u32::try_from(body.len()).map_err(|_| NativeSessionError::Capacity)?;
        let total = add(add(HEAD, body.len())?, 32)?;
        if total > MAX_RECORD_BYTES {
            return Err(NativeSessionError::Capacity);
        }
        let mut bytes = reserved::<u8>(total)?;
        bytes.extend_from_slice(&MAGIC);
        bytes.extend_from_slice(&VERSION.to_le_bytes());
        bytes.extend_from_slice(&self.ledger.tenant.0);
        bytes.extend_from_slice(&self.ledger.session.0);
        bytes.extend_from_slice(&self.ordinal.to_le_bytes());
        bytes.extend_from_slice(&length.to_le_bytes());
        bytes.extend_from_slice(&body);
        let digest = blake3::Hasher::new_derive_key(HASH_DOMAIN)
            .update(&bytes)
            .finalize();
        bytes.extend_from_slice(digest.as_bytes());
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, NativeSessionError> {
        if bytes.len() > MAX_RECORD_BYTES || bytes.len() < add(HEAD, 32)? {
            return Err(NativeSessionError::Corrupt);
        }
        let (payload, digest) = bytes
            .split_at_checked(bytes.len().saturating_sub(32))
            .ok_or(NativeSessionError::Corrupt)?;
        let expected = blake3::Hasher::new_derive_key(HASH_DOMAIN)
            .update(payload)
            .finalize();
        if digest != expected.as_bytes() {
            return Err(NativeSessionError::Corrupt);
        }
        let take = |from: usize, len: usize| -> Result<&[u8], NativeSessionError> {
            payload
                .get(from..add(from, len)?)
                .ok_or(NativeSessionError::Corrupt)
        };
        let fixed = |from: usize| -> Result<[u8; 16], NativeSessionError> {
            take(from, 16)?
                .try_into()
                .map_err(|_| NativeSessionError::Corrupt)
        };
        if take(0, 8)? != MAGIC || take(8, 2)? != VERSION.to_le_bytes() {
            return Err(NativeSessionError::Corrupt);
        }
        let ledger = LedgerId {
            tenant: focal_model::TenantId(fixed(10)?),
            session: focal_model::SessionId(fixed(26)?),
        };
        let ordinal = u64::from_le_bytes(
            take(42, 8)?
                .try_into()
                .map_err(|_| NativeSessionError::Corrupt)?,
        );
        let length = u32::from_le_bytes(
            take(50, 4)?
                .try_into()
                .map_err(|_| NativeSessionError::Corrupt)?,
        );
        let body = take(
            HEAD,
            usize::try_from(length).map_err(|_| NativeSessionError::Corrupt)?,
        )?;
        if add(HEAD, body.len())? != payload.len() {
            return Err(NativeSessionError::Corrupt);
        }
        let (operation, rest): (RangeOperation, _) =
            postcard::take_from_bytes(body).map_err(|_| NativeSessionError::Corrupt)?;
        if !rest.is_empty() || ledger.tenant.is_zero() || ledger.session.is_zero() {
            return Err(NativeSessionError::Corrupt);
        }
        Ok(Self {
            ledger,
            ordinal,
            operation,
        })
    }
}

/// An attestation bound to this session's genesis over the exact proof
/// fields: what every replica can recompute, since the genesis is a committed
/// fact and the proof is the record's own content.
fn attest<T: Serialize>(
    domain: &'static str,
    genesis: &ContentHash,
    value: &T,
) -> Result<ContentHash, NativeSessionError> {
    let bytes = postcard::to_allocvec(value).map_err(|_| NativeSessionError::Corrupt)?;
    let mut hasher = blake3::Hasher::new_derive_key(domain);
    hasher.update(&genesis.0);
    hasher.update(&bytes);
    Ok(ContentHash(*hasher.finalize().as_bytes()))
}

/// The verifier every replica applies movement records under (25 §6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LedgerRangeVerifier {
    genesis: ContentHash,
}
impl LedgerRangeVerifier {
    pub fn new(genesis: ContentHash) -> Self {
        Self { genesis }
    }
    pub fn genesis(&self) -> ContentHash {
        self.genesis
    }
    fn check(&self, expected: ContentHash, actual: ContentHash) -> Result<(), RangeError> {
        if expected == actual {
            Ok(())
        } else {
            Err(RangeError::Unverified)
        }
    }
    pub fn attest_commit(&self, proof: &mut CommitProof) -> Result<(), NativeSessionError> {
        proof.attestation = ContentHash([0; 32]);
        proof.attestation = attest(COMMIT_DOMAIN, &self.genesis, proof)?;
        Ok(())
    }
    pub fn attest_source_seal(
        &self,
        proof: &mut SourceSealProof,
    ) -> Result<(), NativeSessionError> {
        proof.attestation = ContentHash([0; 32]);
        proof.attestation = attest(SEAL_DOMAIN, &self.genesis, proof)?;
        Ok(())
    }
    pub fn attest_ready(&self, proof: &mut DestinationReady) -> Result<(), NativeSessionError> {
        proof.attestation = ContentHash([0; 32]);
        proof.attestation = attest(READY_DOMAIN, &self.genesis, proof)?;
        Ok(())
    }
    pub fn attest_progress(&self, proof: &mut RangeProgress) -> Result<(), NativeSessionError> {
        proof.attestation = ContentHash([0; 32]);
        proof.attestation = attest(PROGRESS_DOMAIN, &self.genesis, proof)?;
        Ok(())
    }
    pub fn attest_read(&self, proof: &mut ReadAvailability) -> Result<(), NativeSessionError> {
        proof.attestation = ContentHash([0; 32]);
        proof.attestation = attest(READ_DOMAIN, &self.genesis, proof)?;
        Ok(())
    }
    pub fn attest_recovery(&self, proof: &mut RecoveryProof) -> Result<(), NativeSessionError> {
        proof.attestation = ContentHash([0; 32]);
        proof.attestation = attest(RECOVERY_DOMAIN, &self.genesis, proof)?;
        Ok(())
    }
    fn verify<T: Serialize + Clone>(
        &self,
        domain: &'static str,
        proof: &T,
        attestation: ContentHash,
        clear: impl FnOnce(&mut T),
    ) -> Result<(), RangeError> {
        let mut bare = proof.clone();
        clear(&mut bare);
        let expected = attest(domain, &self.genesis, &bare).map_err(|_| RangeError::Codec)?;
        self.check(expected, attestation)
    }
}
impl RangeVerifier for LedgerRangeVerifier {
    fn commit(&self, proof: &CommitProof) -> Result<(), RangeError> {
        self.verify(COMMIT_DOMAIN, proof, proof.attestation, |p| {
            p.attestation = ContentHash([0; 32]);
        })
    }
    fn batch(&self, _: &RangeBatch, proof: &CommitProof) -> Result<(), RangeError> {
        self.commit(proof)
    }
    fn source_seal(&self, proof: &SourceSealProof) -> Result<(), RangeError> {
        self.verify(SEAL_DOMAIN, proof, proof.attestation, |p| {
            p.attestation = ContentHash([0; 32]);
        })
    }
    fn destination_ready(&self, proof: &DestinationReady) -> Result<(), RangeError> {
        self.verify(READY_DOMAIN, proof, proof.attestation, |p| {
            p.attestation = ContentHash([0; 32]);
        })
    }
    fn recovery(&self, proof: &RecoveryProof) -> Result<(), RangeError> {
        self.verify(RECOVERY_DOMAIN, proof, proof.attestation, |p| {
            p.attestation = ContentHash([0; 32]);
        })
    }
    fn progress(&self, proof: &RangeProgress) -> Result<(), RangeError> {
        self.verify(PROGRESS_DOMAIN, proof, proof.attestation, |p| {
            p.attestation = ContentHash([0; 32]);
        })
    }
    fn read(&self, proof: &ReadAvailability) -> Result<(), RangeError> {
        self.verify(READ_DOMAIN, proof, proof.attestation, |p| {
            p.attestation = ContentHash([0; 32]);
        })
    }
}

/// The storage key a member boundary sits at: the least key of the affinity.
pub fn boundary_key(affinity: [u8; 16]) -> StorageKey {
    StorageKey::bucket(affinity, 0, 0)
}

/// The movement map of a layout (25 §6): one descriptor per member in key
/// order, every member held by the voters, at range epoch one.
pub fn map_from_layout(
    ledger: LedgerId,
    boundaries: impl Iterator<Item = (RangeId, Option<[u8; 16]>)>,
    limits: RangeLimits,
) -> Result<RangeMap, NativeSessionError> {
    let members: Vec<(RangeId, Option<[u8; 16]>)> = boundaries.collect();
    let mut ranges = reserved::<RangeDescriptor>(members.len())?;
    for (position, (id, start)) in members.iter().enumerate() {
        let end = members
            .get(position.saturating_add(1))
            .and_then(|(_, start)| start.map(boundary_key));
        ranges.push(RangeDescriptor {
            id: *id,
            generation: 1,
            span: KeySpan {
                start: start.map(boundary_key),
                end,
            },
            meta: Placement::voters(),
        });
    }
    RangeMap::new(ledger, RouteEpoch(1), ranges, limits).map_err(NativeSessionError::Range)
}

/// The session's movement coordinator and what the authority has in flight.
pub(crate) struct Movement {
    pub(crate) coordinator: RangeCoordinator,
    pub(crate) verifier: LedgerRangeVerifier,
    pub(crate) limits: RangeLimits,
    /// The record this authority proposed and has not seen applied: its
    /// ordinal and command hash.
    pub(crate) in_flight: Option<(u64, ContentHash)>,
    /// Committed records the state refused, deterministically on every replica.
    pub(crate) refusals: u64,
    pub(crate) last_refusal: Option<RangeError>,
}

impl Movement {
    fn incarnation(genesis: &ContentHash) -> ControllerIncarnation {
        let digest = blake3::derive_key("focal.native.range.incarnation.v1", &genesis.0);
        let mut bytes = [0u8; 16];
        for (target, source) in bytes.iter_mut().zip(digest.iter()) {
            *target = *source;
        }
        ControllerIncarnation(bytes)
    }
    /// A fresh coordinator over `map` for a session whose genesis is `genesis`.
    pub(crate) fn new(
        genesis: ContentHash,
        map: RangeMap,
        limits: RangeLimits,
        budget: MemoryBudget,
    ) -> Result<Self, NativeSessionError> {
        let coordinator = RangeCoordinator::new(map, Self::incarnation(&genesis), limits, budget)?;
        Ok(Self {
            coordinator,
            verifier: LedgerRangeVerifier::new(genesis),
            limits,
            in_flight: None,
            refusals: 0,
            last_refusal: None,
        })
    }
    /// The coordinator restored from a checkpoint's movement section.
    pub(crate) fn restore(
        genesis: ContentHash,
        bytes: &[u8],
        limits: RangeLimits,
        budget: MemoryBudget,
    ) -> Result<Self, NativeSessionError> {
        if bytes.len() > limits.max_checkpoint_bytes {
            return Err(NativeSessionError::Capacity);
        }
        let (state, rest): (RangeCheckpoint, _) =
            postcard::take_from_bytes(bytes).map_err(|_| NativeSessionError::Corrupt)?;
        if !rest.is_empty() {
            return Err(NativeSessionError::Corrupt);
        }
        let coordinator =
            RangeCoordinator::restore(state, Self::incarnation(&genesis), limits, budget)
                .map_err(|_| NativeSessionError::Corrupt)?;
        Ok(Self {
            coordinator,
            verifier: LedgerRangeVerifier::new(genesis),
            limits,
            in_flight: None,
            refusals: 0,
            last_refusal: None,
        })
    }
    /// The coordinator state as the checkpoint carries it.
    pub(crate) fn checkpoint_bytes(&self) -> Result<Vec<u8>, NativeSessionError> {
        let state = self.coordinator.checkpoint();
        let bytes = postcard::to_allocvec(state).map_err(|_| NativeSessionError::Corrupt)?;
        if bytes.len() > self.limits.max_checkpoint_bytes {
            return Err(NativeSessionError::Capacity);
        }
        Ok(bytes)
    }
    pub(crate) fn pending(&self) -> Option<&TransferState> {
        self.coordinator.pending()
    }
    /// The members a pending transfer moves, once its barrier is committed:
    /// the sources and the replacements. Empty before the barrier.
    pub(crate) fn fenced_members(&self) -> BTreeSet<RangeId> {
        let mut fenced = BTreeSet::new();
        if let Some(pending) = self.coordinator.pending()
            && pending.barrier.is_some()
        {
            fenced.extend(pending.intent.sources.iter().copied());
            fenced.extend(pending.intent.replacements.iter().map(|range| range.id));
        }
        fenced
    }
    /// Re-lay the map after a committed split at `at` of the member holding
    /// it: the parent keeps its identity below `at`, `id` names the member
    /// from `at` on; both carry the parent's placement.
    pub(crate) fn split(&mut self, at: [u8; 16], id: RangeId) -> Result<(), NativeSessionError> {
        let key = boundary_key(at);
        let parent = self
            .coordinator
            .map()
            .route(key)
            .cloned()
            .ok_or(NativeSessionError::Corrupt)?;
        let left = RangeDescriptor {
            id: parent.id,
            generation: parent
                .generation
                .checked_add(1)
                .ok_or(NativeSessionError::Capacity)?,
            span: KeySpan {
                start: parent.span.start,
                end: Some(key),
            },
            meta: parent.meta.clone(),
        };
        let right = RangeDescriptor {
            id,
            generation: 1,
            span: KeySpan {
                start: Some(key),
                end: parent.span.end,
            },
            meta: parent.meta.clone(),
        };
        self.coordinator
            .relayout(&BTreeSet::from([parent.id]), vec![left, right])
            .map_err(NativeSessionError::Range)
    }
    /// Re-lay the map after a committed merge of member `left` with its
    /// successor, under the left member's identity and placement.
    pub(crate) fn merge(&mut self, left: RangeId) -> Result<(), NativeSessionError> {
        let ranges = self.coordinator.map().ranges();
        let position = ranges
            .iter()
            .position(|range| range.id == left)
            .ok_or(NativeSessionError::Corrupt)?;
        let left_range = ranges
            .get(position)
            .cloned()
            .ok_or(NativeSessionError::Corrupt)?;
        let right_range = ranges
            .get(position.saturating_add(1))
            .cloned()
            .ok_or(NativeSessionError::Corrupt)?;
        let joined = RangeDescriptor {
            id: left_range.id,
            generation: left_range
                .generation
                .checked_add(1)
                .ok_or(NativeSessionError::Capacity)?,
            span: KeySpan {
                start: left_range.span.start,
                end: right_range.span.end,
            },
            meta: left_range.meta.clone(),
        };
        self.coordinator
            .relayout(
                &BTreeSet::from([left_range.id, right_range.id]),
                vec![joined],
            )
            .map_err(NativeSessionError::Range)
    }
    /// Apply one committed record at native prefix `sequence`, minting its
    /// commit proof from the entry. Inert (and counted) when the committed
    /// state refuses it, the same way on every replica.
    pub(crate) fn apply(
        &mut self,
        record: &MovementRecord,
        sequence: SessionSeq,
        index: u64,
        term: u64,
    ) -> Result<bool, NativeSessionError> {
        let prepared = match self.coordinator.prepare(
            record.ordinal,
            sequence,
            record.operation.clone(),
            &self.verifier,
        ) {
            Ok(prepared) => prepared,
            Err(error) => {
                self.note_applied(record.ordinal, None);
                self.refusals = self.refusals.saturating_add(1);
                self.last_refusal = Some(error);
                return Ok(false);
            }
        };
        let mut proof = CommitProof {
            ledger: record.ledger,
            ordinal: prepared.ordinal(),
            sequence,
            index: RaftIndex(index),
            term: RaftTerm(term),
            command: prepared.hash(),
            attestation: ContentHash([0; 32]),
        };
        self.verifier.attest_commit(&mut proof)?;
        let hash = prepared.hash();
        match self.coordinator.publish(prepared, &proof, &self.verifier) {
            Ok(()) => {
                self.note_applied(record.ordinal, Some(hash));
                Ok(true)
            }
            Err(RangeError::Conflict | RangeError::StaleEpoch) => {
                // The same record applied earlier (a duplicate proposal).
                self.note_applied(record.ordinal, Some(hash));
                Ok(false)
            }
            Err(RangeError::Memory(error)) => Err(NativeSessionError::Memory(error)),
            Err(_) => Err(NativeSessionError::Corrupt),
        }
    }
    fn note_applied(&mut self, ordinal: u64, hash: Option<ContentHash>) {
        if self
            .in_flight
            .is_some_and(|(mine, command)| mine == ordinal && hash.is_none_or(|h| h == command))
        {
            self.in_flight = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use focal_ranges::TransferId;

    fn ledger() -> LedgerId {
        LedgerId {
            tenant: focal_model::TenantId::from_u128(3),
            session: focal_model::SessionId::from_u128(4),
        }
    }

    #[test]
    fn movement_records_round_trip_and_refuse_every_forgery() {
        let record = MovementRecord {
            ledger: ledger(),
            ordinal: 7,
            operation: RangeOperation::Barrier {
                operation: TransferId::from_u128(9),
            },
        };
        let bytes = record.encode().unwrap();
        assert!(bytes.starts_with(&MAGIC));
        assert_eq!(MovementRecord::decode(&bytes).unwrap(), record);
        // Every byte of the payload is under the digest; the digest itself,
        // a trailing byte and a truncation are refused; a zero ledger too.
        for offset in 0..bytes.len() {
            let mut corrupt = bytes.clone();
            corrupt[offset] ^= 1;
            assert!(MovementRecord::decode(&corrupt).is_err(), "offset {offset}");
        }
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(MovementRecord::decode(&trailing).is_err());
        assert!(MovementRecord::decode(&bytes[..bytes.len() - 1]).is_err());
        let zero = MovementRecord {
            ledger: LedgerId {
                tenant: focal_model::TenantId::from_u128(0),
                session: focal_model::SessionId::from_u128(4),
            },
            ..record
        };
        assert!(MovementRecord::decode(&zero.encode().unwrap()).is_err());
    }

    #[test]
    fn genesis_attestations_bind_every_proof_field_and_the_genesis() {
        let verifier = LedgerRangeVerifier::new(ContentHash([2; 32]));
        let other = LedgerRangeVerifier::new(ContentHash([3; 32]));
        let mut proof = CommitProof {
            ledger: ledger(),
            ordinal: 1,
            sequence: SessionSeq(5),
            index: RaftIndex(8),
            term: RaftTerm(2),
            command: ContentHash([1; 32]),
            attestation: ContentHash([0; 32]),
        };
        assert!(verifier.commit(&proof).is_err());
        verifier.attest_commit(&mut proof).unwrap();
        verifier.commit(&proof).unwrap();
        assert!(other.commit(&proof).is_err());
        let mut changed = proof.clone();
        changed.term = RaftTerm(3);
        assert!(verifier.commit(&changed).is_err());
        let mut progress = RangeProgress {
            ledger: ledger(),
            epoch: RouteEpoch(1),
            range: RangeId::from_u128(1),
            range_generation: 1,
            replica: focal_ranges::ReplicaId {
                node: 1,
                generation: 1,
            },
            through: SessionSeq(5),
            term: RaftTerm(2),
            root: ContentHash([4; 32]),
            attestation: ContentHash([0; 32]),
        };
        assert!(verifier.progress(&progress).is_err());
        verifier.attest_progress(&mut progress).unwrap();
        verifier.progress(&progress).unwrap();
    }
}
