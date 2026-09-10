use crate::*;
use focal_memory::{
    Allocation, BudgetKind, BudgetLane, Change, Entry, MemoryBudget, PreparedRange, RangeConfig,
    RangeStore, SnapshotLease,
};
use focal_model::{ContentHash, LedgerId, RouteEpoch, SessionSeq};
use serde::{Deserialize, Serialize, ser::SerializeSeq};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataRow {
    pub key: StorageKey,
    pub value: Vec<u8>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeBlock {
    pub index: u32,
    pub rows: Vec<DataRow>,
}
impl RangeBlock {
    pub fn hash(&self) -> Result<ContentHash, RangeError> {
        digest("focal.range-block.v1", self)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockDescriptor {
    pub index: u32,
    pub rows: usize,
    pub bytes: usize,
    pub first: StorageKey,
    pub last: StorageKey,
    pub hash: ContentHash,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeedManifest {
    pub ledger: LedgerId,
    pub operation: TransferId,
    pub source_epoch: RouteEpoch,
    pub target_epoch: RouteEpoch,
    pub destination: RangeDescriptor,
    pub prefix: SessionSeq,
    pub blocks: Vec<BlockDescriptor>,
}
impl SeedManifest {
    pub fn hash(&self) -> Result<ContentHash, RangeError> {
        digest("focal.range-seed.v1", self)
    }
    pub fn validate(&self, limits: RangeLimits) -> Result<(), RangeError> {
        validate_descriptor(&self.destination, limits)?;
        if self.source_epoch.0.checked_add(1) != Some(self.target_epoch.0)
            || self.blocks.len() > limits.max_blocks
        {
            return Err(RangeError::StaleEpoch);
        }
        let mut previous = None;
        let mut total = 0usize;
        for (index, block) in self.blocks.iter().enumerate() {
            if usize::try_from(block.index).ok() != Some(index)
                || block.rows == 0
                || block.rows > limits.max_block_rows
                || block.bytes > limits.max_block_bytes
                || block.first > block.last
                || !self.destination.span.contains(&block.first)
                || !self.destination.span.contains(&block.last)
                || previous.is_some_and(|last| last >= block.first)
                || !nonzero(block.hash)
            {
                return Err(RangeError::Invalid("block manifest"));
            }
            total = add(total, block.rows)?;
            previous = Some(block.last);
        }
        if total > limits.max_replica_rows {
            return Err(RangeError::Capacity);
        }
        Ok(())
    }
}
struct HeldBlock {
    block: RangeBlock,
    _allocation: Allocation,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StageCheckpoint {
    schema: u16,
    manifest: SeedManifest,
    blocks: BTreeMap<u32, RangeBlock>,
}
pub struct RangeStager {
    manifest: SeedManifest,
    blocks: BTreeMap<u32, HeldBlock>,
    limits: RangeLimits,
    budget: MemoryBudget,
    _allocation: Allocation,
}
impl RangeStager {
    pub fn new(
        manifest: SeedManifest,
        limits: RangeLimits,
        budget: MemoryBudget,
    ) -> Result<Self, RangeError> {
        limits.validate()?;
        manifest.validate(limits)?;
        let allocation = budget
            .reserve(
                BudgetKind::Recovery,
                BudgetLane::Completion,
                add(
                    row::<Self>(),
                    mul(manifest.blocks.len(), row::<BlockDescriptor>())?,
                )?,
            )?
            .commit();
        Ok(Self {
            manifest,
            blocks: BTreeMap::new(),
            limits,
            budget,
            _allocation: allocation,
        })
    }
    pub fn manifest(&self) -> &SeedManifest {
        &self.manifest
    }
    pub fn accepted_blocks(&self) -> usize {
        self.blocks.len()
    }
    pub fn accept(&mut self, block: RangeBlock) -> Result<(), RangeError> {
        let expected = self
            .manifest
            .blocks
            .get(block.index as usize)
            .ok_or(RangeError::Missing)?;
        let bytes = postcard::experimental::serialized_size(&block)?;
        if bytes != expected.bytes
            || bytes > self.limits.max_block_bytes
            || block.rows.len() != expected.rows
            || block.hash()? != expected.hash
        {
            return Err(RangeError::Checksum);
        }
        if block.rows.first().map(|row| row.key) != Some(expected.first)
            || block.rows.last().map(|row| row.key) != Some(expected.last)
            || block
                .rows
                .iter()
                .zip(block.rows.iter().skip(1))
                .any(|(left, right)| left.key >= right.key)
        {
            return Err(RangeError::Overlap);
        }
        if let Some(old) = self.blocks.get(&block.index) {
            return if old.block == block {
                Ok(())
            } else {
                Err(RangeError::Conflict)
            };
        }
        let charge = add(
            row::<HeldBlock>(),
            add(
                mul(block.rows.capacity(), size_of::<DataRow>())?,
                block
                    .rows
                    .iter()
                    .try_fold(0usize, |sum, row| add(sum, add(row.value.capacity(), 32)?))?,
            )?,
        )?;
        let allocation = self
            .budget
            .reserve(BudgetKind::Recovery, BudgetLane::Completion, charge)?
            .commit();
        self.blocks.insert(
            block.index,
            HeldBlock {
                block,
                _allocation: allocation,
            },
        );
        Ok(())
    }
    pub fn checkpoint(&self) -> Result<RangeSnapshot, RangeError> {
        let estimate = self.blocks.values().try_fold(
            add(
                8192,
                postcard::experimental::serialized_size(&self.manifest)?,
            )?,
            |sum, block| add(sum, postcard::experimental::serialized_size(&block.block)?),
        )?;
        let allocation = self
            .budget
            .reserve(
                BudgetKind::Recovery,
                BudgetLane::Completion,
                mul(estimate, 8)?,
            )?
            .commit();
        let checkpoint = StageCheckpoint {
            schema: 1,
            manifest: self.manifest.clone(),
            blocks: self
                .blocks
                .iter()
                .map(|(index, held)| (*index, held.block.clone()))
                .collect(),
        };
        RangeSnapshot::encode(&checkpoint, self.limits, allocation)
    }
    pub fn restore(
        bytes: &[u8],
        expected: ContentHash,
        limits: RangeLimits,
        budget: MemoryBudget,
    ) -> Result<Self, RangeError> {
        if ContentHash(blake3::derive_key("focal.range-checkpoint.v1", bytes)) != expected {
            return Err(RangeError::Checksum);
        }
        let _decode = budget.reserve(
            BudgetKind::Recovery,
            BudgetLane::Completion,
            mul(add(bytes.len(), 8192)?, 32)?,
        )?;
        let checkpoint: StageCheckpoint = decode(bytes, limits.max_checkpoint_bytes)?;
        if checkpoint.schema != 1 || checkpoint.blocks.len() > limits.max_blocks {
            return Err(RangeError::Capacity);
        }
        let mut stage = Self::new(checkpoint.manifest, limits, budget.clone())?;
        for (index, block) in checkpoint.blocks {
            if index != block.index {
                return Err(RangeError::Conflict);
            }
            stage.accept(block)?;
        }
        Ok(stage)
    }
    pub fn install(self, incarnation: u128) -> Result<RangeReplica, RangeError> {
        if self.blocks.len() != self.manifest.blocks.len() {
            return Err(RangeError::NotReady);
        }
        let iterator = self
            .blocks
            .values()
            .flat_map(|held| held.block.rows.iter())
            .map(|row| {
                Entry::new(
                    row.key,
                    row.value.clone(),
                    row.value.capacity().saturating_add(32),
                )
            });
        let store = RangeStore::from_entries(
            focal_memory::RangeId(incarnation),
            self.manifest.prefix.0,
            store_config(self.limits),
            self.budget.clone(),
            iterator,
        )?;
        let role = ReplicaRole::Staging {
            operation: self.manifest.operation,
            source_epoch: self.manifest.source_epoch,
            target_epoch: self.manifest.target_epoch,
        };
        RangeReplica::from_store(
            self.manifest.ledger,
            self.manifest.destination.clone(),
            role,
            self.manifest.prefix,
            self.manifest.hash()?,
            store,
            BTreeMap::new(),
            None,
            self.limits,
            self.budget.clone(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RangeWrite {
    Put(DataRow),
    Delete(StorageKey),
}
impl RangeWrite {
    fn key(&self) -> StorageKey {
        match self {
            Self::Put(row) => row.key,
            Self::Delete(key) => *key,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeBatch {
    pub ledger: LedgerId,
    pub epoch: RouteEpoch,
    pub range: RangeId,
    pub range_generation: u64,
    pub sequence: SessionSeq,
    pub transaction: TransactionId,
    pub predecessor_reads: ContentHash,
    pub decision: ContentHash,
    pub writes: Vec<RangeWrite>,
}
impl RangeBatch {
    pub fn hash(&self) -> Result<ContentHash, RangeError> {
        digest("focal.range-batch.v1", self)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaRole {
    Active {
        epoch: RouteEpoch,
    },
    Staging {
        operation: TransferId,
        source_epoch: RouteEpoch,
        target_epoch: RouteEpoch,
    },
    Sealed {
        operation: TransferId,
        epoch: RouteEpoch,
        next_epoch: RouteEpoch,
        cut: SessionSeq,
    },
    Retired,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct Armed {
    operation: TransferId,
    next_epoch: RouteEpoch,
    intent: ContentHash,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ReplicaCheckpoint {
    schema: u16,
    ledger: LedgerId,
    descriptor: RangeDescriptor,
    role: ReplicaRole,
    seed: SessionSeq,
    snapshot: ContentHash,
    prefix: SessionSeq,
    rows: Vec<DataRow>,
    receipts: BTreeMap<SessionSeq, ContentHash>,
    armed: Option<Armed>,
}
/// Owns an accounted serialized checkpoint. Disk custody is established by the
/// host writing/syncing it and issuing a verified receipt, not by construction.
pub struct RangeSnapshot {
    bytes: Vec<u8>,
    hash: ContentHash,
    _allocation: Allocation,
}
impl RangeSnapshot {
    fn encode(
        value: &impl Serialize,
        limits: RangeLimits,
        allocation: Allocation,
    ) -> Result<Self, RangeError> {
        let bytes = encode(value, limits.max_checkpoint_bytes)?;
        let hash = ContentHash(blake3::derive_key("focal.range-checkpoint.v1", &bytes));
        Ok(Self {
            bytes,
            hash,
            _allocation: allocation,
        })
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn hash(&self) -> ContentHash {
        self.hash
    }
}
pub struct PreparedReplicaBatch {
    base_role: ReplicaRole,
    sequence: SessionSeq,
    hash: ContentHash,
    decision: ContentHash,
    prepared: Option<PreparedRange<StorageKey, Vec<u8>>>,
    receipts: BTreeMap<SessionSeq, ContentHash>,
    batch: RangeBatch,
    _allocation: Allocation,
}
pub struct RangeReplica {
    ledger: LedgerId,
    descriptor: RangeDescriptor,
    role: ReplicaRole,
    seed: SessionSeq,
    snapshot: ContentHash,
    store: RangeStore<StorageKey, Vec<u8>>,
    receipts: BTreeMap<SessionSeq, ContentHash>,
    armed: Option<Armed>,
    limits: RangeLimits,
    budget: MemoryBudget,
    _allocation: Allocation,
}
impl RangeReplica {
    pub fn empty(
        ledger: LedgerId,
        descriptor: RangeDescriptor,
        epoch: RouteEpoch,
        incarnation: u128,
        limits: RangeLimits,
        budget: MemoryBudget,
    ) -> Result<Self, RangeError> {
        validate_descriptor(&descriptor, limits)?;
        let store = RangeStore::new(
            focal_memory::RangeId(incarnation),
            0,
            store_config(limits),
            budget.clone(),
        )?;
        Self::from_store(
            ledger,
            descriptor,
            ReplicaRole::Active { epoch },
            SessionSeq(0),
            ContentHash([0; 32]),
            store,
            BTreeMap::new(),
            None,
            limits,
            budget,
        )
    }
    #[allow(
        clippy::too_many_arguments,
        reason = "private validated checkpoint/seed construction"
    )]
    fn from_store(
        ledger: LedgerId,
        descriptor: RangeDescriptor,
        role: ReplicaRole,
        seed: SessionSeq,
        snapshot: ContentHash,
        store: RangeStore<StorageKey, Vec<u8>>,
        receipts: BTreeMap<SessionSeq, ContentHash>,
        armed: Option<Armed>,
        limits: RangeLimits,
        budget: MemoryBudget,
    ) -> Result<Self, RangeError> {
        limits.validate()?;
        let allocation = budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                add(
                    row::<Self>(),
                    mul(limits.max_receipts, row::<(SessionSeq, ContentHash)>())?,
                )?,
            )?
            .commit();
        Ok(Self {
            ledger,
            descriptor,
            role,
            seed,
            snapshot,
            store,
            receipts,
            armed,
            limits,
            budget,
            _allocation: allocation,
        })
    }
    pub fn descriptor(&self) -> &RangeDescriptor {
        &self.descriptor
    }
    pub fn role(&self) -> ReplicaRole {
        self.role
    }
    pub fn prefix(&self) -> SessionSeq {
        SessionSeq(self.store.prefix())
    }
    pub fn get(&self, key: StorageKey) -> Option<&[u8]> {
        self.store.get(&key).map(Vec::as_slice)
    }
    pub fn rows(&self) -> impl Iterator<Item = (&StorageKey, &[u8])> {
        self.store
            .entries()
            .map(|entry| (&entry.key, entry.value.as_slice()))
    }
    pub fn state_hash(&self) -> Result<ContentHash, RangeError> {
        digest(
            "focal.range-state.v1",
            &(self.ledger, self.descriptor.span, Rows(&self.store)),
        )
    }
    pub fn prepare_batch(&self, batch: &RangeBatch) -> Result<PreparedReplicaBatch, RangeError> {
        if batch.ledger != self.ledger {
            return Err(RangeError::WrongLedger);
        }
        if batch.range != self.descriptor.id || batch.range_generation != self.descriptor.generation
        {
            return Err(RangeError::Generation);
        }
        let epoch = match self.role {
            ReplicaRole::Active { epoch } | ReplicaRole::Sealed { epoch, .. } => epoch,
            ReplicaRole::Staging { source_epoch, .. } => source_epoch,
            ReplicaRole::Retired => return Err(RangeError::Sealed),
        };
        if batch.epoch != epoch {
            return Err(RangeError::StaleEpoch);
        }
        if batch.writes.len() > self.limits.max_batch_writes
            || batch
                .writes
                .iter()
                .any(|write| !self.descriptor.span.contains(&write.key()))
        {
            return Err(RangeError::Capacity);
        }
        if batch
            .writes
            .iter()
            .zip(batch.writes.iter().skip(1))
            .any(|(left, right)| left.key() >= right.key())
        {
            return Err(RangeError::Overlap);
        }
        if matches!(self.role, ReplicaRole::Sealed { .. }) && !batch.writes.is_empty() {
            return Err(RangeError::Sealed);
        }
        let hash = batch.hash()?;
        let duplicate = if batch.sequence <= self.prefix() {
            match self.receipts.get(&batch.sequence) {
                Some(old) if *old == hash => true,
                Some(_) => return Err(RangeError::Conflict),
                None => return Err(RangeError::RetryTooOld),
            }
        } else {
            if self.store.prefix().checked_add(1) != Some(batch.sequence.0) {
                return Err(RangeError::Gap);
            }
            false
        };
        let bytes = add(
            mul(
                add(postcard::experimental::serialized_size(batch)?, 8192)?,
                16,
            )?,
            mul(self.limits.max_receipts, row::<(SessionSeq, ContentHash)>())?,
        )?;
        let allocation = self
            .budget
            .reserve(BudgetKind::Pending, BudgetLane::Completion, bytes)?
            .commit();
        let mut receipts = self.receipts.clone();
        let prepared = if duplicate {
            None
        } else {
            let changes: Vec<_> = batch
                .writes
                .iter()
                .map(|write| match write {
                    RangeWrite::Put(row) => Change::Put(Entry::new(
                        row.key,
                        row.value.clone(),
                        row.value.capacity().saturating_add(32),
                    )),
                    RangeWrite::Delete(key) => Change::Delete(*key),
                })
                .collect();
            let prepared =
                self.store
                    .prepare_batch(batch.sequence.0, changes, BudgetLane::Completion)?;
            if prepared.len() > self.limits.max_replica_rows {
                return Err(RangeError::Capacity);
            }
            receipts.insert(batch.sequence, hash);
            while receipts.len() > self.limits.max_receipts {
                receipts.pop_first();
            }
            Some(prepared)
        };
        Ok(PreparedReplicaBatch {
            base_role: self.role,
            sequence: batch.sequence,
            hash,
            decision: batch.decision,
            prepared,
            receipts,
            batch: batch.clone(),
            _allocation: allocation,
        })
    }
    pub fn publish_batch(
        &mut self,
        prepared: PreparedReplicaBatch,
        proof: &CommitProof,
        verifier: &impl RangeVerifier,
    ) -> Result<(), RangeError> {
        if self.role != prepared.base_role {
            return Err(RangeError::StalePreparation);
        }
        verify_commit(
            proof,
            self.ledger,
            0,
            prepared.sequence,
            prepared.decision,
            verifier,
        )?;
        verifier.batch(&prepared.batch, proof)?;
        if let Some(root) = prepared.prepared {
            self.store.publish(root)?;
            self.receipts = prepared.receipts;
        } else if self.receipts.get(&prepared.sequence) != Some(&prepared.hash) {
            return Err(RangeError::StalePreparation);
        }
        Ok(())
    }
    pub fn arm(
        &mut self,
        certificate: &IntentCertificate,
        verifier: &impl RangeVerifier,
    ) -> Result<(), RangeError> {
        let ReplicaRole::Active { epoch } = self.role else {
            return Err(RangeError::Phase);
        };
        if certificate.intent.ledger != self.ledger
            || certificate.intent.old_epoch != epoch
            || !certificate.intent.sources.contains(&self.descriptor.id)
        {
            return Err(RangeError::WrongOwner);
        }
        let hash = range_command_hash(
            self.ledger,
            certificate.commit.ordinal,
            certificate.commit.sequence,
            &RangeOperation::Begin(certificate.intent.clone()),
        )?;
        verify_commit(
            &certificate.commit,
            self.ledger,
            certificate.commit.ordinal,
            certificate.commit.sequence,
            hash,
            verifier,
        )?;
        let armed = Armed {
            operation: certificate.intent.operation,
            next_epoch: RouteEpoch(epoch.0.checked_add(1).ok_or(RangeError::Overflow)?),
            intent: digest("focal.range-intent.v1", &certificate.intent)?,
        };
        if self.armed.is_some_and(|old| old != armed) {
            return Err(RangeError::Conflict);
        }
        self.armed = Some(armed);
        Ok(())
    }
    /// Persist a checkpoint containing this seal before a source-seal proof may
    /// claim durable fencing. New data writes are rejected immediately.
    pub fn seal(
        &mut self,
        certificate: &BarrierCertificate,
        verifier: &impl RangeVerifier,
    ) -> Result<(), RangeError> {
        let armed = self.armed.ok_or(RangeError::Phase)?;
        if armed.operation != certificate.intent.operation
            || armed.intent != digest("focal.range-intent.v1", &certificate.intent)?
        {
            return Err(RangeError::Conflict);
        }
        let role = ReplicaRole::Sealed {
            operation: armed.operation,
            epoch: certificate.intent.old_epoch,
            next_epoch: armed.next_epoch,
            cut: certificate.commit.sequence,
        };
        if self.role == role {
            return Ok(());
        }
        if !matches!(self.role, ReplicaRole::Active { .. })
            || self.prefix() != certificate.commit.sequence
        {
            return Err(RangeError::NotReady);
        }
        verify_commit(
            &certificate.commit,
            self.ledger,
            certificate.commit.ordinal,
            certificate.commit.sequence,
            range_command_hash(
                self.ledger,
                certificate.commit.ordinal,
                certificate.commit.sequence,
                &RangeOperation::Barrier {
                    operation: armed.operation,
                },
            )?,
            verifier,
        )?;
        self.role = role;
        Ok(())
    }
    pub fn source_seal_request(
        &self,
        checkpoint: ContentHash,
    ) -> Result<SourceSealProof, RangeError> {
        let ReplicaRole::Sealed {
            operation,
            epoch,
            cut,
            ..
        } = self.role
        else {
            return Err(RangeError::Phase);
        };
        Ok(SourceSealProof {
            ledger: self.ledger,
            operation,
            old_epoch: epoch,
            range: self.descriptor.id,
            range_generation: self.descriptor.generation,
            replica: self.descriptor.meta.replica_owner()?,
            cut,
            checkpoint,
            attestation: ContentHash([0; 32]),
        })
    }
    pub fn ready_request(&self, checkpoint: ContentHash) -> Result<DestinationReady, RangeError> {
        let ReplicaRole::Staging {
            operation,
            target_epoch,
            ..
        } = self.role
        else {
            return Err(RangeError::Phase);
        };
        Ok(DestinationReady {
            ledger: self.ledger,
            operation,
            new_epoch: target_epoch,
            range: self.descriptor.id,
            range_generation: self.descriptor.generation,
            replica: self.descriptor.meta.replica_owner()?,
            seed: self.seed,
            through: self.prefix(),
            snapshot: self.snapshot,
            state: self.state_hash()?,
            checkpoint,
            attestation: ContentHash([0; 32]),
        })
    }
    pub fn activate(
        &mut self,
        certificate: &ActivationCertificate,
        verifier: &impl RangeVerifier,
    ) -> Result<(), RangeError> {
        certificate.verify(self.limits, verifier)?;
        let target = certificate
            .map
            .get(self.descriptor.id)
            .ok_or(RangeError::WrongOwner)?;
        if *target != self.descriptor || certificate.intent.ledger != self.ledger {
            return Err(RangeError::Generation);
        }
        if self.role
            == (ReplicaRole::Active {
                epoch: certificate.map.epoch(),
            })
        {
            return Ok(());
        }
        let expected = ReplicaRole::Staging {
            operation: certificate.intent.operation,
            source_epoch: certificate.intent.old_epoch,
            target_epoch: certificate.map.epoch(),
        };
        if self.role != expected {
            return Err(RangeError::Phase);
        }
        let ready = certificate
            .destinations
            .get(&self.descriptor.id)
            .ok_or(RangeError::NotReady)?;
        if ready.state != self.state_hash()?
            || ready.snapshot != self.snapshot
            || ready.through > self.prefix()
            || ready.through < certificate.barrier.sequence
        {
            return Err(RangeError::Checksum);
        }
        self.role = ReplicaRole::Active {
            epoch: certificate.map.epoch(),
        };
        self.armed = None;
        Ok(())
    }
    pub fn check_serving(
        &self,
        replica: ReplicaId,
        epoch: RouteEpoch,
        prefix: SessionSeq,
        new_token: bool,
    ) -> Result<(), RangeError> {
        if self.descriptor.meta.owner != Holder::Replica(replica) {
            return Err(RangeError::Generation);
        }
        match self.role {
            ReplicaRole::Active { epoch: current }
                if epoch == current && prefix <= self.prefix() =>
            {
                Ok(())
            }
            ReplicaRole::Sealed {
                epoch: old, cut, ..
            } if !new_token && epoch == old && prefix <= cut => Ok(()),
            _ => Err(RangeError::WrongOwner),
        }
    }
    pub fn pin_current(
        &mut self,
        now: u64,
        ttl: u64,
    ) -> Result<SnapshotLease<StorageKey, Vec<u8>>, RangeError> {
        Ok(self.store.pin(now, ttl)?)
    }
    pub fn advance_clock(&mut self, now: u64) -> Result<usize, RangeError> {
        Ok(self.store.advance_clock(now)?)
    }
    pub fn checkpoint(&self) -> Result<RangeSnapshot, RangeError> {
        let estimate = self.store.entries().try_fold(8192usize, |sum, entry| {
            add(sum, add(size_of::<DataRow>(), entry.value.capacity())?)
        })?;
        let allocation = self
            .budget
            .reserve(
                BudgetKind::Recovery,
                BudgetLane::Completion,
                add(
                    mul(estimate, 8)?,
                    mul(self.receipts.len(), row::<(SessionSeq, ContentHash)>())?,
                )?,
            )?
            .commit();
        let checkpoint = ReplicaCheckpoint {
            schema: 1,
            ledger: self.ledger,
            descriptor: self.descriptor.clone(),
            role: self.role,
            seed: self.seed,
            snapshot: self.snapshot,
            prefix: self.prefix(),
            rows: self
                .store
                .entries()
                .map(|entry| DataRow {
                    key: entry.key,
                    value: entry.value.clone(),
                })
                .collect(),
            receipts: self.receipts.clone(),
            armed: self.armed,
        };
        RangeSnapshot::encode(&checkpoint, self.limits, allocation)
    }
    pub fn restore(
        bytes: &[u8],
        expected: ContentHash,
        incarnation: u128,
        limits: RangeLimits,
        budget: MemoryBudget,
    ) -> Result<Self, RangeError> {
        if ContentHash(blake3::derive_key("focal.range-checkpoint.v1", bytes)) != expected {
            return Err(RangeError::Checksum);
        }
        let _decode = budget.reserve(
            BudgetKind::Recovery,
            BudgetLane::Completion,
            mul(add(bytes.len(), 8192)?, 32)?,
        )?;
        let checkpoint: ReplicaCheckpoint = decode(bytes, limits.max_checkpoint_bytes)?;
        validate_descriptor(&checkpoint.descriptor, limits)?;
        if checkpoint.schema != 1
            || checkpoint.rows.len() > limits.max_replica_rows
            || checkpoint.receipts.len() > limits.max_receipts
            || checkpoint.seed > checkpoint.prefix
            || checkpoint
                .rows
                .iter()
                .any(|row| !checkpoint.descriptor.span.contains(&row.key))
            || checkpoint
                .rows
                .iter()
                .zip(checkpoint.rows.iter().skip(1))
                .any(|(left, right)| left.key >= right.key)
        {
            return Err(RangeError::Invalid("replica checkpoint"));
        }
        if checkpoint
            .receipts
            .keys()
            .any(|sequence| *sequence > checkpoint.prefix)
        {
            return Err(RangeError::StaleEpoch);
        }
        if let ReplicaRole::Sealed {
            cut,
            operation,
            next_epoch,
            ..
        } = checkpoint.role
            && (cut > checkpoint.prefix
                || checkpoint.armed.is_none_or(|armed| {
                    armed.operation != operation || armed.next_epoch != next_epoch
                }))
        {
            return Err(RangeError::Conflict);
        }
        let rows = checkpoint.rows.into_iter().map(|row| {
            let heap = row.value.capacity().saturating_add(32);
            Entry::new(row.key, row.value, heap)
        });
        let store = RangeStore::from_entries(
            focal_memory::RangeId(incarnation),
            checkpoint.prefix.0,
            store_config(limits),
            budget.clone(),
            rows,
        )?;
        Self::from_store(
            checkpoint.ledger,
            checkpoint.descriptor,
            checkpoint.role,
            checkpoint.seed,
            checkpoint.snapshot,
            store,
            checkpoint.receipts,
            checkpoint.armed,
            limits,
            budget.clone(),
        )
    }
}
struct Rows<'a>(&'a RangeStore<StorageKey, Vec<u8>>);
impl Serialize for Rows<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for row in self.0.entries() {
            seq.serialize_element(&(&row.key, &row.value))?;
        }
        seq.end()
    }
}
fn store_config(limits: RangeLimits) -> RangeConfig {
    RangeConfig {
        max_batch_entries: limits.max_batch_writes,
        max_snapshot_leases: limits.max_pins,
        max_snapshot_ttl: limits.max_pin_ttl,
        ..Default::default()
    }
}
