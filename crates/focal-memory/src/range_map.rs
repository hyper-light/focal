//! Key spans and a validated, gap-free map of ranges over one ordered key
//! space (25 §3). The map is a pure value: it routes a key to the range that
//! holds it, proves that a set of spans covers the key space exactly once and
//! derives a successor map from one contiguous replacement (a move, a split
//! or a merge). Its keys are the store's keys, so one model describes the
//! native layout (`Key` in storage order) and a fixed-width transfer key;
//! ownership, readers and every other placement fact ride in each
//! descriptor's `meta`, which this module never interprets.

use crate::RangeId;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// A half-open span of keys. `start` is the least key the span holds, or
/// `None` for the least key of all; `end` is the first key past the span, or
/// `None` when the span runs to the end of the key space.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct KeySpan<K> {
    pub start: Option<K>,
    pub end: Option<K>,
}

impl<K> KeySpan<K> {
    /// The whole key space.
    pub const fn all() -> Self {
        Self {
            start: None,
            end: None,
        }
    }
}

impl<K: Ord> KeySpan<K> {
    /// A span holds at least one key: a bounded start lies before a bounded
    /// end.
    pub fn validate(&self) -> Result<(), RangeMapError> {
        match (&self.start, &self.end) {
            (Some(start), Some(end)) if start >= end => {
                Err(RangeMapError::Invalid("empty or reversed span"))
            }
            _ => Ok(()),
        }
    }
    pub fn contains(&self, key: &K) -> bool {
        self.start.as_ref().is_none_or(|start| key >= start)
            && self.end.as_ref().is_none_or(|end| key < end)
    }
    /// The keys both spans hold, if any.
    pub fn intersection(&self, other: &Self) -> Option<Self>
    where
        K: Clone,
    {
        let start = match (&self.start, &other.start) {
            (Some(a), Some(b)) => Some(a.max(b).clone()),
            (Some(a), None) | (None, Some(a)) => Some(a.clone()),
            (None, None) => None,
        };
        let end = match (&self.end, &other.end) {
            (Some(a), Some(b)) => Some(a.min(b).clone()),
            (Some(a), None) | (None, Some(a)) => Some(a.clone()),
            (None, None) => None,
        };
        let span = Self { start, end };
        span.validate().ok().map(|()| span)
    }
}

/// One range of a map: its identity, the generation of that identity (one
/// per replacement that keeps the identity) and the span it holds.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct RangeDescriptor<K, M> {
    pub id: RangeId,
    pub generation: u64,
    pub span: KeySpan<K>,
    pub meta: M,
}

impl<K: Ord, M> RangeDescriptor<K, M> {
    fn validate(&self) -> Result<(), RangeMapError> {
        self.span.validate()?;
        if self.generation == 0 {
            return Err(RangeMapError::Generation);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RangeMapLimits {
    pub max_ranges: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeMapError {
    Invalid(&'static str),
    Capacity,
    Overflow,
    Generation,
    Gap,
    Overlap,
    Missing,
    Conflict,
}

impl std::fmt::Display for RangeMapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for RangeMapError {}

/// Ranges in key order covering the key space exactly once, at one epoch.
/// Immutable after construction: a successor comes from [`RangeMap::replace`]
/// and carries the next epoch. Deserialize only through a validated owner, or
/// call [`RangeMap::validate`] before trusting independently decoded bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct RangeMap<K, M> {
    epoch: u64,
    ranges: Vec<RangeDescriptor<K, M>>,
}

impl<K: Ord, M> RangeMap<K, M> {
    pub fn new(
        epoch: u64,
        ranges: Vec<RangeDescriptor<K, M>>,
        limits: RangeMapLimits,
    ) -> Result<Self, RangeMapError> {
        let map = Self { epoch, ranges };
        map.validate(limits)?;
        Ok(map)
    }
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
    pub fn ranges(&self) -> &[RangeDescriptor<K, M>] {
        &self.ranges
    }
    pub fn len(&self) -> usize {
        self.ranges.len()
    }
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }
    pub fn into_parts(self) -> (u64, Vec<RangeDescriptor<K, M>>) {
        (self.epoch, self.ranges)
    }
    pub fn position(&self, id: RangeId) -> Option<usize> {
        self.ranges.iter().position(|range| range.id == id)
    }
    pub fn get(&self, id: RangeId) -> Option<&RangeDescriptor<K, M>> {
        self.position(id).and_then(|index| self.ranges.get(index))
    }
    /// The index of the range holding `key`: the last range whose start is
    /// at or below it. A valid map holds every key, so this is only `None`
    /// for an empty (invalid) map.
    pub fn route(&self, key: &K) -> Option<usize> {
        let index = self
            .ranges
            .partition_point(|range| range.span.start.as_ref().is_none_or(|start| start <= key))
            .checked_sub(1)?;
        self.ranges
            .get(index)
            .filter(|range| range.span.contains(key))
            .map(|_| index)
    }
    pub fn routed(&self, key: &K) -> Option<&RangeDescriptor<K, M>> {
        self.route(key).and_then(|index| self.ranges.get(index))
    }
    /// Every structural rule: a nonzero epoch, one to `max_ranges` ranges,
    /// distinct identities, nonzero generations, the first range from the
    /// least key, each next range starting exactly where the previous ends,
    /// the last running to the end.
    pub fn validate(&self, limits: RangeMapLimits) -> Result<(), RangeMapError> {
        if limits.max_ranges == 0 {
            return Err(RangeMapError::Invalid("zero range limit"));
        }
        if self.epoch == 0 || self.ranges.is_empty() {
            return Err(RangeMapError::Invalid("empty map or zero epoch"));
        }
        if self.ranges.len() > limits.max_ranges {
            return Err(RangeMapError::Capacity);
        }
        let mut expected_start: Option<Option<&K>> = Some(None);
        for (index, range) in self.ranges.iter().enumerate() {
            range.validate()?;
            if self
                .ranges
                .get(..index)
                .is_some_and(|earlier| earlier.iter().any(|other| other.id == range.id))
            {
                return Err(RangeMapError::Conflict);
            }
            let start = range.span.start.as_ref();
            match expected_start {
                None => return Err(RangeMapError::Overlap),
                Some(expected) => match (expected, start) {
                    (None, None) => {}
                    (Some(a), Some(b)) if a == b => {}
                    (None, Some(_)) => return Err(RangeMapError::Gap),
                    (Some(_), None) => return Err(RangeMapError::Overlap),
                    (Some(a), Some(b)) if b < a => return Err(RangeMapError::Overlap),
                    (Some(_), Some(_)) => return Err(RangeMapError::Gap),
                },
            }
            expected_start = range.span.end.as_ref().map(Some);
        }
        if expected_start.is_some() {
            return Err(RangeMapError::Gap);
        }
        Ok(())
    }

    /// The successor map after one contiguous set of `sources` is replaced by
    /// `replacements` covering exactly the same keys: a move keeps one span,
    /// a split yields several, a merge one. A replacement that keeps an
    /// identity advances its generation by one; a new identity starts at
    /// generation one. The epoch advances by one.
    pub fn replace(
        &self,
        sources: &[RangeId],
        replacements: Vec<RangeDescriptor<K, M>>,
        limits: RangeMapLimits,
    ) -> Result<Self, RangeMapError>
    where
        K: Clone,
        M: Clone,
    {
        if sources.is_empty() || replacements.is_empty() || replacements.len() > limits.max_ranges {
            return Err(RangeMapError::Invalid("replacement set"));
        }
        let positions: Vec<usize> = self
            .ranges
            .iter()
            .enumerate()
            .filter(|(_, range)| sources.contains(&range.id))
            .map(|(index, _)| index)
            .collect();
        let distinct = sources.iter().enumerate().all(|(index, id)| {
            !sources
                .get(..index)
                .is_some_and(|earlier| earlier.contains(id))
        });
        if !distinct || positions.len() != sources.len() {
            return Err(RangeMapError::Missing);
        }
        let first = *positions.first().ok_or(RangeMapError::Missing)?;
        let last = *positions.last().ok_or(RangeMapError::Missing)?;
        if last
            .checked_sub(first)
            .and_then(|count| count.checked_add(1))
            .ok_or(RangeMapError::Overflow)?
            != sources.len()
        {
            return Err(RangeMapError::Gap);
        }
        let source_first = self.ranges.get(first).ok_or(RangeMapError::Missing)?;
        let source_last = self.ranges.get(last).ok_or(RangeMapError::Missing)?;
        if replacements
            .first()
            .ok_or(RangeMapError::Missing)?
            .span
            .start
            != source_first.span.start
            || replacements.last().ok_or(RangeMapError::Missing)?.span.end != source_last.span.end
        {
            return Err(RangeMapError::Gap);
        }
        for replacement in &replacements {
            if let Some(old) = self.get(replacement.id) {
                if !sources.contains(&replacement.id)
                    || old.generation.checked_add(1) != Some(replacement.generation)
                {
                    return Err(RangeMapError::Generation);
                }
            } else if replacement.generation != 1 {
                return Err(RangeMapError::Generation);
            }
        }
        let mut ranges = Vec::new();
        let count = self
            .ranges
            .len()
            .checked_sub(sources.len())
            .and_then(|kept| kept.checked_add(replacements.len()))
            .ok_or(RangeMapError::Overflow)?;
        ranges
            .try_reserve_exact(count)
            .map_err(|_| RangeMapError::Capacity)?;
        ranges.extend(
            self.ranges
                .get(..first)
                .ok_or(RangeMapError::Missing)?
                .iter()
                .cloned(),
        );
        ranges.extend(replacements);
        ranges.extend(
            self.ranges
                .get(last.checked_add(1).ok_or(RangeMapError::Overflow)?..)
                .ok_or(RangeMapError::Missing)?
                .iter()
                .cloned(),
        );
        Self::new(
            self.epoch.checked_add(1).ok_or(RangeMapError::Overflow)?,
            ranges,
            limits,
        )
    }
}

#[cfg(test)]
#[path = "range_map_tests.rs"]
mod tests;
