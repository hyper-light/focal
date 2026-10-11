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
