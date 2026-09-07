//! One checked byte/count partition rule shared by quoting and construction.
//! Layout limits are immutable for an owner's lifetime; no value is copied here.

use super::{Change, Entry, Page, RangeConfig, page_charge};
use crate::{MemoryError, checked_add};

pub(super) fn validate<K, V>(config: RangeConfig) -> Result<(), MemoryError> {
    if config.page_bytes < page_charge::<K, V>(1, 0)?
        || config.max_entry_bytes < size_of::<Entry<K, V>>()
    {
        return Err(MemoryError::InvalidConfiguration(
            "range byte limits cannot hold one empty entry",
        ));
    }
    Ok(())
}

pub(super) fn check_entry<K, V>(heap: usize, config: RangeConfig) -> Result<usize, MemoryError> {
    let bytes = checked_add(size_of::<Entry<K, V>>(), heap)?;
    if bytes > config.max_entry_bytes {
        return Err(MemoryError::ItemTooLarge {
            bytes,
            limit: config.max_entry_bytes,
        });
    }
    Ok(bytes)
}

#[derive(Clone, Copy)]
pub(super) struct LeafSpan {
    pub start: usize,
    pub len: usize,
    pub heap: usize,
}

pub(super) struct LeafPartition {
    config: RangeConfig,
    base_bytes: usize,
    entry_bytes: usize,
    span: LeafSpan,
    bytes: usize,
}

impl LeafPartition {
    pub fn new<K, V>(config: RangeConfig) -> Result<Self, MemoryError> {
        let base_bytes = page_charge::<K, V>(0, 0)?;
        Ok(Self {
            config,
            base_bytes,
            entry_bytes: size_of::<Entry<K, V>>(),
            span: LeafSpan {
                start: 0,
                len: 0,
                heap: 0,
            },
            bytes: base_bytes,
        })
    }

    pub fn push(
        &mut self,
        heap: usize,
        emit: &mut impl FnMut(LeafSpan) -> Result<(), MemoryError>,
    ) -> Result<(), MemoryError> {
        let bytes = checked_add(self.entry_bytes, heap)?;
        if bytes > self.config.max_entry_bytes {
            return Err(MemoryError::ItemTooLarge {
                bytes,
                limit: self.config.max_entry_bytes,
            });
        }
        if self.span.len != 0
            && (self.span.len == self.config.page_entries
                || bytes > self.config.page_bytes.saturating_sub(self.bytes))
        {
            self.finish(emit)?;
        }
        self.span.len = checked_add(self.span.len, 1)?;
        self.span.heap = checked_add(self.span.heap, heap)?;
        self.bytes = checked_add(self.bytes, bytes)?;
        // Oversized entries are emitted alone, never combined with a neighbor.
        if self.bytes > self.config.page_bytes {
            self.finish(emit)?;
        }
        Ok(())
    }

    pub fn finish(
        &mut self,
        emit: &mut impl FnMut(LeafSpan) -> Result<(), MemoryError>,
    ) -> Result<(), MemoryError> {
        if self.span.len == 0 {
            return Ok(());
        }
        emit(self.span)?;
        self.span.start = checked_add(self.span.start, self.span.len)?;
        self.span.len = 0;
        self.span.heap = 0;
        self.bytes = self.base_bytes;
        Ok(())
    }
}

/// Return the boundary between smaller and larger incoming keys when this
/// oversized singleton is unchanged. Only counts escape, so the builder can
/// consume the input iterator afterward. Replacement/deletion uses normal merge.
pub(super) fn reusable_singleton<K: Ord, V>(
    page: &Page<K, V>,
    changes: &[Change<K, V>],
    config: RangeConfig,
) -> Result<Option<usize>, MemoryError> {
    let [entry] = page.entries.as_slice() else {
        return Ok(None);
    };
    if page_charge::<K, V>(1, entry.heap_bytes)? <= config.page_bytes {
        return Ok(None);
    }
    Ok(changes
        .binary_search_by(|change| change.key().cmp(&entry.key))
        .err())
}
