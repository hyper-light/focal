#![cfg_attr(
    test,
    allow(
        clippy::panic,
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::unreachable,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros
    )
)]
//! Single-owner, RAM-primary storage primitives.
//!
//! Stable keys, explicit prefixes, and caller-supplied owner identities are the
//! durable-facing boundary. Arena handles and snapshot leases are local only.
//! This crate performs no IO, reads no ambient clock, and creates no background
//! tasks. Immutable pages may cross threads; mutable indexes have one owner.
//!
//! Memory accounting covers storage-owned allocations. The caller must include
//! the heap capacity of keys and values in their supplied payload charge (and
//! account shared content separately). Arbitrary `Clone` implementations cannot
//! be measured automatically. Allocator bookkeeping is conservatively charged;
//! allocator fragmentation and allocations retained by callers are not RSS.

mod arena;
mod budget;
mod content;
mod disk;
#[cfg(test)]
mod disk_tests;
#[cfg(test)]
mod funded_budget_tests;
mod index;
mod key_set;
#[cfg(test)]
mod owned_range_tests;
mod owner;
mod range;
mod range_key;
pub use range_key::RangeKey;
#[cfg(test)]
mod range_elastic_tests;
mod range_map;
#[cfg(feature = "serde")]
pub mod serde_bytes;
pub mod short_copy;
mod snapshot;
mod traversal;

pub use arena::{Arena, ArenaConfig, ArenaId, Handle};
pub use budget::{
    Allocation, BudgetKind, BudgetLane, BudgetRefusals, BudgetStats, ElasticFundedPool,
    MemoryBudget, Reservation,
};
pub use content::{ImmutableContent, VersionedRecord};
pub use disk::{
    DISK_KIND_COUNT, DiskBudget, DiskBudgetConfig, DiskKind, DiskReservation, DiskStats,
};
pub use index::StableIndex;
pub use key_set::{
    FrozenKeySet, KeyEdit, KeyIter, KeySet, KeySetBuilder, KeySetFunding, KeySetStats,
    MAX_KEY as MAX_SET_KEY, SetEdit, SetKey, WeakKeySet,
};
pub use owner::OwnerId;
pub use range::{
    Change, Entry, FrozenRange, PreparedRange, RangeConfig, RangeHydration, RangeHydrationLimits,
    RangeHydrationLookup, RangeHydrationSource, RangeHydrationView, RangeId,
    RangePreparationCharges, RangePreparationPlan, RangeStats, RangeStore, RangeWriteEnvelope,
    RangeWriteLimits, Rewrite, SparseBase, SparseRange, SparseRefusal,
};
pub use range_map::{KeySpan, RangeDescriptor, RangeMap, RangeMapError, RangeMapLimits};
pub use snapshot::{LeaseHold, ReadBudget, ReadPage, ScanContinuation, ScanQuery, SnapshotLease};
pub use traversal::{
    TraversalContinuation, TraversalLimits, TraversalPage, TraversalQuery, TraversalStop,
};

/// All operational failures are typed; failed operations leave published state
/// unchanged. Allocation failure is reported when `try_reserve` can detect it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MemoryError {
    InvalidConfiguration(&'static str),
    Capacity {
        requested: usize,
        available: usize,
    },
    /// A volume cannot promise the bytes a durable write needs; retryable
    /// once space is reclaimed or the sample changes.
    DiskCapacity {
        requested: u64,
        available: u64,
    },
    AllocationFailed,
    CounterExhausted(&'static str),
    WrongArena,
    WrongRange,
    StaleHandle,
    StalePreparation {
        prepared_at: u64,
        current_prefix: u64,
    },
    DuplicateKey,
    MissingKey,
    PrefixMismatch {
        expected: u64,
        actual: u64,
    },
    ClockRegression {
        current: u64,
        supplied: u64,
    },
    LeaseExpired,
    WrongLease,
    QueryMismatch,
    ItemTooLarge {
        bytes: usize,
        limit: usize,
    },
    InvalidNeighbors,
}

impl std::fmt::Display for MemoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for MemoryError {}

pub(crate) fn checked_add(left: usize, right: usize) -> Result<usize, MemoryError> {
    left.checked_add(right)
        .ok_or(MemoryError::CounterExhausted("byte charge"))
}

pub(crate) fn checked_mul(left: usize, right: usize) -> Result<usize, MemoryError> {
    left.checked_mul(right)
        .ok_or(MemoryError::CounterExhausted("byte charge"))
}

/// Conservative bookkeeping for each allocation the engine itself creates.
pub const ALLOCATOR_OVERHEAD: usize = 4 * std::mem::size_of::<usize>();

/// What one entry of a `BTreeMap<K, V>` holds at most, its share of its node
/// included: std's nodes hold eleven entries and every node but the root is
/// kept at least five full, so an entry's share of its node is at most eleven
/// fifths of an entry, and of the node's twelve links, its header and its
/// allocation a fifth: three entries' bytes and one allocation's bookkeeping
/// bound it. The heap a key or value holds itself is its owner's to add.
pub fn map_entry_bytes<K, V>() -> Option<usize> {
    std::mem::size_of::<K>()
        .checked_add(std::mem::size_of::<V>())?
        .checked_mul(3)?
        .checked_add(ALLOCATOR_OVERHEAD)
}

/// What a `BTreeMap<K, V>` (or a `BTreeSet<K>`, whose values take no room)
/// of `entries` holds at most: each entry's share (`map_entry_bytes`) and one
/// whole node besides, for the root, the one node that may be less than five
/// full. An empty map allocates nothing.
pub fn map_bytes<K, V>(entries: usize) -> Option<usize> {
    if entries == 0 {
        return Some(0);
    }
    // A node: eleven entries, twelve links, its parent link, index and length.
    let node = std::mem::size_of::<K>()
        .checked_add(std::mem::size_of::<V>())?
        .checked_mul(11)?
        .checked_add(std::mem::size_of::<usize>().checked_mul(14)?)?
        .checked_add(ALLOCATOR_OVERHEAD)?;
    entries
        .checked_mul(map_entry_bytes::<K, V>()?)?
        .checked_add(node)
}
