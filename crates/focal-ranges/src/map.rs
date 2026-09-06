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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeySpan {
    pub start: StorageKey,
    pub end: Option<StorageKey>,
}
impl KeySpan {
    pub const fn all() -> Self {
        Self {
            start: StorageKey::MIN,
            end: None,
        }
    }
    pub fn validate(self) -> Result<(), RangeError> {
        if self.end.is_some_and(|end| end <= self.start) {
            Err(RangeError::Invalid("empty/reversed span"))
        } else {
            Ok(())
        }
    }
    pub fn contains(self, key: StorageKey) -> bool {
        key >= self.start && self.end.is_none_or(|end| key < end)
    }
    pub fn intersection(self, other: Self) -> Option<Self> {
        let span = Self {
            start: self.start.max(other.start),
            end: match (self.end, other.end) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, None) | (None, a) => a,
            },
        };
        span.validate().ok().map(|()| span)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeDescriptor {
    pub id: RangeId,
    pub generation: u64,
    pub span: KeySpan,
    pub owner: ReplicaId,
    pub readers: BTreeSet<ReplicaId>,
}
impl RangeDescriptor {
    pub(crate) fn validate(&self, limits: RangeLimits) -> Result<(), RangeError> {
        self.span.validate()?;
        self.owner.validate()?;
        if self.generation == 0 {
            return Err(RangeError::Generation);
        }
        if self.readers.len() > limits.max_readers {
            return Err(RangeError::Capacity);
        }
        for reader in &self.readers {
            reader.validate()?;
        }
        Ok(())
    }
    pub fn accepts_reader(&self, replica: ReplicaId) -> bool {
        self.owner == replica || self.readers.contains(&replica)
    }
}
/// Immutable after construction. Deserialize only through a validated owner or
/// call validate before trusting independently decoded map bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeMap {
    ledger: LedgerId,
    epoch: RouteEpoch,
    ranges: Vec<RangeDescriptor>,
}
impl RangeMap {
    pub fn new(
        ledger: LedgerId,
        epoch: RouteEpoch,
        ranges: Vec<RangeDescriptor>,
        limits: RangeLimits,
    ) -> Result<Self, RangeError> {
        let map = Self {
            ledger,
            epoch,
            ranges,
        };
        map.validate(limits)?;
        Ok(map)
    }
    pub fn ledger(&self) -> LedgerId {
        self.ledger
    }
    pub fn epoch(&self) -> RouteEpoch {
        self.epoch
    }
    pub fn ranges(&self) -> &[RangeDescriptor] {
        &self.ranges
    }
    pub fn get(&self, id: RangeId) -> Option<&RangeDescriptor> {
        self.ranges.iter().find(|range| range.id == id)
    }
    pub fn route(&self, key: StorageKey) -> Option<&RangeDescriptor> {
        self.ranges
            .get(
                self.ranges
                    .partition_point(|range| range.span.start <= key)
                    .saturating_sub(1),
            )
            .filter(|range| range.span.contains(key))
    }
    pub fn hash(&self) -> Result<ContentHash, RangeError> {
        digest("focal.range-map.v1", self)
    }
    pub fn validate(&self, limits: RangeLimits) -> Result<(), RangeError> {
        limits.validate()?;
        if self.epoch.0 == 0 || self.ranges.is_empty() {
            return Err(RangeError::Invalid("empty map or zero epoch"));
        }
        if self.ranges.len() > limits.max_ranges {
            return Err(RangeError::Capacity);
        }
        let mut next = Some(StorageKey::MIN);
        let mut ids = BTreeSet::new();
        for range in &self.ranges {
            range.validate(limits)?;
            if !ids.insert(range.id) {
                return Err(RangeError::Conflict);
            }
            match next {
                None => return Err(RangeError::Overlap),
                Some(key) if range.span.start < key => return Err(RangeError::Overlap),
                Some(key) if range.span.start > key => return Err(RangeError::Gap),
                _ => {}
            }
            next = range.span.end;
        }
        if next.is_some() {
            return Err(RangeError::Gap);
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
        if sources.is_empty() || replacements.is_empty() || replacements.len() > limits.max_ranges {
            return Err(RangeError::Invalid("replacement set"));
        }
        let positions: Vec<_> = self
            .ranges
            .iter()
            .enumerate()
            .filter(|(_, range)| sources.contains(&range.id))
            .map(|(index, _)| index)
            .collect();
        if positions.len() != sources.len() {
            return Err(RangeError::Missing);
        }
        let first = *positions.first().ok_or(RangeError::Missing)?;
        let last = *positions.last().ok_or(RangeError::Missing)?;
        if last
            .checked_sub(first)
            .and_then(|count| count.checked_add(1))
            .ok_or(RangeError::Overflow)?
            != sources.len()
        {
            return Err(RangeError::Gap);
        }
        if replacements.first().ok_or(RangeError::Missing)?.span.start
            != self
                .ranges
                .get(first)
                .ok_or(RangeError::Missing)?
                .span
                .start
            || replacements.last().ok_or(RangeError::Missing)?.span.end
                != self.ranges.get(last).ok_or(RangeError::Missing)?.span.end
        {
            return Err(RangeError::Gap);
        }
        for replacement in &replacements {
            if let Some(old) = self.get(replacement.id) {
                if !sources.contains(&replacement.id)
                    || old.generation.checked_add(1) != Some(replacement.generation)
                {
                    return Err(RangeError::Generation);
                }
            } else if replacement.generation != 1 {
                return Err(RangeError::Generation);
            }
        }
        let mut ranges = self
            .ranges
            .get(..first)
            .ok_or(RangeError::Missing)?
            .to_vec();
        ranges.extend(replacements);
        ranges.extend_from_slice(
            self.ranges
                .get(last.checked_add(1).ok_or(RangeError::Overflow)?..)
                .ok_or(RangeError::Missing)?,
        );
        Self::new(
            self.ledger,
            RouteEpoch(self.epoch.0.checked_add(1).ok_or(RangeError::Overflow)?),
            ranges,
            limits,
        )
    }
    pub(crate) fn charge(&self) -> Result<usize, RangeError> {
        let mut bytes = add(
            256,
            mul(self.ranges.capacity(), size_of::<RangeDescriptor>())?,
        )?;
        for range in &self.ranges {
            bytes = add(bytes, mul(range.readers.len(), row::<ReplicaId>())?)?;
        }
        Ok(bytes)
    }
}
