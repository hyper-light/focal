//! Select only affected leaves using the immutable base's boundaries. The
//! returned counts retain no borrow of the incoming owned change iterator.

use super::{Change, Root};
use crate::{MemoryError, checked_add};

#[derive(Clone, Copy)]
pub(super) struct LeafGroup {
    pub rank: usize,
    pub count: usize,
}

pub(super) fn next<K: Ord, V>(
    base: &Root<K, V>,
    remaining: &[Change<K, V>],
) -> Result<Option<LeafGroup>, MemoryError> {
    let Some(first) = remaining.first() else {
        return Ok(None);
    };
    if base.pages.is_empty() {
        return Ok(Some(LeafGroup {
            rank: 0,
            count: remaining.len(),
        }));
    }
    let rank = base.pages.page_index(first.key());
    let next_first = base
        .pages
        .get(checked_add(rank, 1)?)
        .and_then(|page| page.entries.first())
        .map(|entry| &entry.key);
    let count = remaining
        .partition_point(|change| next_first.is_none_or(|boundary| change.key() < boundary));
    if count == 0 || base.pages.get(rank).is_none() {
        return Err(MemoryError::InvalidConfiguration(
            "range directory did not select a nonempty change group",
        ));
    }
    Ok(Some(LeafGroup { rank, count }))
}
