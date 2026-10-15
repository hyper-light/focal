//! Select only affected leaves using the immutable base's boundaries. The
//! returned counts retain no borrow of the incoming owned change iterator.

use super::{Change, Page, Root};
use crate::MemoryError;
use std::sync::Arc;

/// The changes that go in one page: its rank in the base and the page (none
/// in an empty base), and how many of the remaining changes it takes.
#[derive(Clone, Copy)]
pub(super) struct LeafGroup<'a, K, V> {
    pub rank: usize,
    pub count: usize,
    pub page: Option<&'a Arc<Page<K, V>>>,
}

pub(super) fn next<'a, K: crate::RangeKey, V>(
    base: &'a Root<K, V>,
    remaining: &[Change<K, V>],
) -> Result<Option<LeafGroup<'a, K, V>>, MemoryError> {
    let Some(first) = remaining.first() else {
        return Ok(None);
    };
    if base.pages.is_empty() {
        return Ok(Some(LeafGroup {
            rank: 0,
            count: remaining.len(),
            page: None,
        }));
    }
    let located = base
        .pages
        .locate(first.key())
        .ok_or(MemoryError::MissingKey)?;
    let next_first = located
        .next
        .and_then(|page| page.entries.first())
        .map(|entry| &entry.key);
    let count = remaining
        .partition_point(|change| next_first.is_none_or(|boundary| change.key() < boundary));
    if count == 0 {
        return Err(MemoryError::InvalidConfiguration(
            "range directory did not select a nonempty change group",
        ));
    }
    Ok(Some(LeafGroup {
        rank: located.rank,
        count,
        page: Some(located.page),
    }))
}

/// The changes that go in the page of the last remaining change, which are
/// the last `count` of `remaining`: groups taken from the end, so a
/// publication takes each group's changes off the end of its batch and
/// edits pages from the last, never moving a change or a page it has yet to
/// reach.
pub(super) fn last<'a, K: crate::RangeKey, V>(
    base: &'a Root<K, V>,
    remaining: &[Change<K, V>],
) -> Result<Option<LeafGroup<'a, K, V>>, MemoryError> {
    let Some(last) = remaining.last() else {
        return Ok(None);
    };
    if base.pages.is_empty() {
        return Ok(Some(LeafGroup {
            rank: 0,
            count: remaining.len(),
            page: None,
        }));
    }
    let located = base
        .pages
        .locate(last.key())
        .ok_or(MemoryError::MissingKey)?;
    // The first page also takes every key before its own first.
    let count = if located.rank == 0 {
        remaining.len()
    } else {
        let first = located
            .page
            .entries
            .first()
            .map(|entry| &entry.key)
            .ok_or(MemoryError::MissingKey)?;
        remaining
            .len()
            .saturating_sub(remaining.partition_point(|change| change.key() < first))
    };
    if count == 0 {
        return Err(MemoryError::InvalidConfiguration(
            "range directory did not select a nonempty change group",
        ));
    }
    Ok(Some(LeafGroup {
        rank: located.rank,
        count,
        page: Some(located.page),
    }))
}
