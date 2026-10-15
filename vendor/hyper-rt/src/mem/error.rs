//! The closed refusal taxonomy of the memory crate (§4.2, failure matrix): every variant names
//! what was asked and what was available, so the caller can decide; none is a catch-all.

use std::fmt;

/// A typed refusal from the memory crate; never a panic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemError {
    /// The handle's slot has been freed (its generation moved on) or never existed.
    StaleHandle {
        /// The slot index the handle named.
        index: u32,
        /// The generation the handle carried.
        generation: u32,
    },
    /// The slab has no free slot and no reserve segment to take one from.
    SlabFull {
        /// The slab's capacity in slots.
        capacity: usize,
    },
    /// No region has a free extent of the requested class; retryable after growth.
    ArenaExhausted {
        /// The bytes requested.
        requested: usize,
        /// The largest free extent any region can offer right now, in bytes.
        largest_free: usize,
    },
    /// A reservation exceeds what the shard's reserve can cover; nothing was allocated.
    BudgetExceeded {
        /// The bytes requested.
        requested: u64,
        /// The bytes available.
        available: u64,
    },
    /// The OS refused to lock some of the requested bytes; the rest stays usable, unlocked.
    LockRefused {
        /// The bytes asked to be locked.
        requested: usize,
        /// The bytes the OS locked before refusing.
        locked: usize,
        /// The OS error code, when one exists.
        code: Option<i32>,
    },
    /// The OS refused to map a region.
    RegionRefused {
        /// The bytes requested.
        len: usize,
        /// The OS error code, when one exists.
        code: Option<i32>,
    },
    /// A request exceeds the largest class the arena serves.
    TooLarge {
        /// The bytes requested.
        len: usize,
        /// The largest bytes any single extent can hold.
        max: usize,
    },
    /// A ring slot count that is not a power of two, or zero.
    BadCapacity {
        /// The capacity given.
        capacity: usize,
    },
    /// The OS refused a call on a shared memory object; the call is named.
    OsRefused {
        /// The call.
        call: &'static str,
        /// The OS error code, when one exists.
        code: Option<i32>,
    },
    /// An offset into a shared object that is misaligned or past its end.
    OutOfRange {
        /// The offset asked for.
        offset: usize,
        /// The object's length.
        len: usize,
    },
    /// A free that names no live block this allocator handed out (AUD-29-10): nothing changed.
    ForeignExtent {
        /// The offset the freed extent named.
        offset: usize,
        /// The length the freed extent named.
        len: usize,
        /// Why it names no live block.
        reason: ExtentRefusal,
    },
    /// A shared object's layout or an access to it that would mix a plain access with an atomic one, or two
    /// atomic widths, on the same bytes (AUD-29-09): nothing was read, written or viewed.
    LayoutRefused {
        /// The offset refused.
        offset: usize,
        /// The bytes refused.
        len: usize,
        /// Why.
        reason: LayoutRefusal,
    },
    /// A block head's or a slot's generation space is spent: it is retired rather than wrapped, so a stale
    /// reference to it can never be mistaken for a live one (AUD-29-11).
    GenerationExhausted {
        /// The granule or slot index whose generations are spent.
        index: u32,
    },
    /// A region added under an id the arena already holds (A-98): nothing changed.
    RegionOccupied {
        /// The id.
        region: u16,
    },
    /// A region removed while it still holds a live or deferred block (A-98): nothing changed.
    RegionInUse {
        /// The id.
        region: u16,
        /// The bytes still allocated in it.
        allocated: usize,
    },
}

/// Why a shared object's layout or access was refused (AUD-29-09).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LayoutRefusal {
    /// A declared word, or an access, past the object's end.
    OutOfRange,
    /// A declared word not aligned to its width.
    Misaligned,
    /// Two declared words sharing a byte (a stride shorter than the word included).
    Overlap,
    /// An atomic view of bytes that are not a declared word of that width.
    NotAWord,
    /// A copy that would touch a declared word.
    TouchesWord,
}

/// Why a freed extent names no live block (AUD-29-10).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExtentRefusal {
    /// Another arena issued it.
    OtherArena,
    /// Its region is not one of this arena's.
    NoSuchRegion,
    /// Its offset is not on a granule, or not on its block size's boundary.
    Misaligned,
    /// Its length is not exactly one block's (a power-of-two number of granules).
    WrongLength,
    /// It lies past the region's end.
    OutOfRange,
    /// No live block starts there with that length: it is free (a duplicate free) or inside another block.
    NotAllocated,
    /// The block there was freed and handed out again since: the extent is a stale copy.
    Stale,
    /// A claim names a span that is already allocated, wholly or in part (A-64: a recovery image naming one
    /// block twice, or two overlapping blocks).
    Claimed,
}

impl fmt::Display for MemError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StaleHandle { index, generation } => {
                write!(f, "stale handle: slot {index} generation {generation}")
            }
            Self::SlabFull { capacity } => write!(f, "slab full at {capacity} slots"),
            Self::ArenaExhausted {
                requested,
                largest_free,
            } => {
                write!(
                    f,
                    "arena exhausted: {requested} bytes requested, largest free extent {largest_free}"
                )
            }
            Self::BudgetExceeded {
                requested,
                available,
            } => {
                write!(
                    f,
                    "budget exceeded: {requested} bytes requested, {available} available"
                )
            }
            Self::LockRefused {
                requested,
                locked,
                code,
            } => {
                write!(
                    f,
                    "lock refused after {locked} of {requested} bytes (code {code:?})"
                )
            }
            Self::RegionRefused { len, code } => {
                write!(f, "region of {len} bytes refused (code {code:?})")
            }
            Self::TooLarge { len, max } => {
                write!(f, "{len} bytes exceeds the largest extent of {max}")
            }
            Self::BadCapacity { capacity } => {
                write!(f, "ring capacity {capacity} is not a power of two")
            }
            Self::OsRefused { call, code } => write!(f, "{call} refused (code {code:?})"),
            Self::OutOfRange { offset, len } => {
                write!(
                    f,
                    "offset {offset} is misaligned or past the {len}-byte object"
                )
            }
            Self::ForeignExtent {
                offset,
                len,
                reason,
            } => write!(
                f,
                "extent at {offset} of {len} bytes names no live block: {reason:?}"
            ),
            Self::LayoutRefused {
                offset,
                len,
                reason,
            } => write!(
                f,
                "shared layout refused at {offset} for {len} bytes: {reason:?}"
            ),
            Self::RegionOccupied { region } => write!(f, "region {region} is already held"),
            Self::RegionInUse { region, allocated } => {
                write!(f, "region {region} still holds {allocated} allocated bytes")
            }
            Self::GenerationExhausted { index } => {
                write!(f, "generations of index {index} are spent; it is retired")
            }
        }
    }
}

impl std::error::Error for MemError {}
