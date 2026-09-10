use crate::*;
use focal_model::{ClaimId, ContentHash, LedgerId, ObjectId, RouteEpoch};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Stable logical affinity comes first. Families distinguish primary objects,
/// adjacency, lifecycle cells, and explicit shardable secondary-index buckets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct StorageKey {
    pub affinity: [u8; 16],
    pub family: u16,
    pub object: [u8; 16],
    pub slot: u64,
}
impl StorageKey {
    pub const MIN: Self = Self {
        affinity: [0; 16],
        family: 0,
        object: [0; 16],
        slot: 0,
    };
    pub fn claim(claim: ClaimId, family: u16, object: ObjectId, slot: u64) -> Self {
        Self {
            affinity: claim.0,
            family,
            object: object.0,
            slot,
        }
    }
    pub fn bucket(affinity: [u8; 16], family: u16, slot: u64) -> Self {
        Self {
            affinity,
            family,
            object: [0; 16],
            slot,
        }
    }
}

/// The span, descriptor and map model is the memory crate's generic one
/// (`focal_memory::range_map`), keyed by the transfer key here and by the
/// native key in the native engine; placement facts ride in `Placement`.
pub type KeySpan = focal_memory::KeySpan<StorageKey>;
pub type RangeDescriptor = focal_memory::RangeDescriptor<StorageKey, Placement>;

/// Who holds a range: the session's voters (the log itself, which
/// materializes every member to admit mutations; 25 §6) or one materializer
/// replica. Proofs are demanded only of replica holders.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Holder {
    Voters,
    Replica(ReplicaId),
}
impl Holder {
    pub fn replica(self) -> Option<ReplicaId> {
        match self {
            Self::Voters => None,
            Self::Replica(replica) => Some(replica),
        }
    }
    pub(crate) fn validate(self) -> Result<(), RangeError> {
        match self {
            Self::Voters => Ok(()),
            Self::Replica(replica) => replica.validate(),
        }
    }
}
/// Who holds a range and which other replicas may serve reads of it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Placement {
    pub owner: Holder,
    pub readers: BTreeSet<ReplicaId>,
}
impl Placement {
    /// Held by the voters alone.
    pub fn voters() -> Self {
        Self {
            owner: Holder::Voters,
            readers: BTreeSet::new(),
        }
    }
    /// Held by one replica alone.
    pub fn replica(replica: ReplicaId) -> Self {
        Self {
            owner: Holder::Replica(replica),
            readers: BTreeSet::new(),
        }
    }
    pub fn accepts_reader(&self, replica: ReplicaId) -> bool {
        self.owner == Holder::Replica(replica) || self.readers.contains(&replica)
    }
    /// The replica holding this range; a voter-held range has none.
    pub fn replica_owner(&self) -> Result<ReplicaId, RangeError> {
        self.owner.replica().ok_or(RangeError::WrongOwner)
    }
}

pub(crate) fn validate_descriptor(
    descriptor: &RangeDescriptor,
    limits: RangeLimits,
) -> Result<(), RangeError> {
    descriptor.span.validate()?;
    descriptor.meta.owner.validate()?;
    if descriptor.generation == 0 {
        return Err(RangeError::Generation);
    }
    if descriptor.meta.readers.len() > limits.max_readers {
        return Err(RangeError::Capacity);
    }
    for reader in &descriptor.meta.readers {
        reader.validate()?;
    }
    Ok(())
}

fn map_limits(limits: RangeLimits) -> focal_memory::RangeMapLimits {
    focal_memory::RangeMapLimits {
        max_ranges: limits.max_ranges,
    }
}

impl From<focal_memory::RangeMapError> for RangeError {
    fn from(value: focal_memory::RangeMapError) -> Self {
        use focal_memory::RangeMapError as E;
        match value {
            E::Invalid(message) => Self::Invalid(message),
            E::Capacity => Self::Capacity,
            E::Overflow => Self::Overflow,
            E::Generation => Self::Generation,
            E::Gap => Self::Gap,
            E::Overlap => Self::Overlap,
            E::Missing => Self::Missing,
            E::Conflict => Self::Conflict,
        }
    }
}

/// Immutable after construction. Deserialize only through a validated owner or
/// call validate before trusting independently decoded map bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeMap {
    ledger: LedgerId,
    map: focal_memory::RangeMap<StorageKey, Placement>,
}
impl RangeMap {
    pub fn new(
        ledger: LedgerId,
        epoch: RouteEpoch,
        ranges: Vec<RangeDescriptor>,
        limits: RangeLimits,
    ) -> Result<Self, RangeError> {
        limits.validate()?;
        let map = Self {
            ledger,
            map: focal_memory::RangeMap::new(epoch.0, ranges, map_limits(limits))?,
        };
        map.validate(limits)?;
        Ok(map)
    }
    pub fn ledger(&self) -> LedgerId {
        self.ledger
    }
    pub fn epoch(&self) -> RouteEpoch {
        RouteEpoch(self.map.epoch())
    }
    pub fn ranges(&self) -> &[RangeDescriptor] {
        self.map.ranges()
    }
    pub fn get(&self, id: RangeId) -> Option<&RangeDescriptor> {
        self.map.get(id)
    }
    pub fn route(&self, key: StorageKey) -> Option<&RangeDescriptor> {
        self.map.routed(&key)
    }
    pub fn hash(&self) -> Result<ContentHash, RangeError> {
        digest("focal.range-map.v1", self)
    }
    pub fn validate(&self, limits: RangeLimits) -> Result<(), RangeError> {
        limits.validate()?;
        self.map.validate(map_limits(limits))?;
        for range in self.ranges() {
            validate_descriptor(range, limits)?;
        }
        Ok(())
    }
    /// A single replacement covers exactly one contiguous set of source
    /// ranges. It expresses move (1->1), split (1->N), or merge (N->1).
    pub fn replace(
        &self,
        sources: &BTreeSet<RangeId>,
        replacements: Vec<RangeDescriptor>,
        limits: RangeLimits,
    ) -> Result<Self, RangeError> {
        let mut ids = Vec::new();
        ids.try_reserve_exact(sources.len())
            .map_err(|_| RangeError::Capacity)?;
        ids.extend(sources.iter().copied());
        let map = Self {
            ledger: self.ledger,
            map: self.map.replace(&ids, replacements, map_limits(limits))?,
        };
        map.validate(limits)?;
        Ok(map)
    }
    pub(crate) fn charge(&self) -> Result<usize, RangeError> {
        let mut bytes = add(256, mul(self.ranges().len(), size_of::<RangeDescriptor>())?)?;
        for range in self.ranges() {
            bytes = add(bytes, mul(range.meta.readers.len(), row::<ReplicaId>())?)?;
        }
        Ok(bytes)
    }
}
