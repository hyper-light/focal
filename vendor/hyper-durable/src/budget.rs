//! Memory reserved from the owner's budget before a transition (focal's R28; `docs/durable.md`
//! §6): an input whose reservation is refused changes nothing. After each call the replica
//! settles what it holds against what it reserved, so the budget follows its resident bytes
//! (the core's and the shell's), not an estimate.
//!
//! An owner that bounds memory some other way passes [`Unbounded`], and then nothing is counted
//! at all: the replica's paths test [`Budget::BOUNDED`], a constant, and the count is compiled
//! out (`docs/durable.md` §14, item 7).

/// An owner's memory budget, shared by the replicas it owns.
pub trait Budget {
    /// Whether the budget counts anything. When false the replica neither reserves nor settles.
    const BOUNDED: bool;

    /// Reserves `bytes` before an input; false when the budget cannot hold them, and then
    /// nothing is reserved.
    fn reserve(&mut self, bytes: u64) -> bool;

    /// Charges `bytes` a replica came to hold past what it reserved: what a transition already
    /// made cannot be refused, only counted, and the next reservation sees it.
    fn charge(&mut self, bytes: u64);

    /// Gives back `bytes`.
    fn release(&mut self, bytes: u64);
}

/// The budget of an owner that bounds memory otherwise: it admits everything and counts nothing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Unbounded;

impl Budget for Unbounded {
    /// Counts nothing: the replica's reservations compile out.
    const BOUNDED: bool = false;
    #[inline]
    fn reserve(&mut self, _bytes: u64) -> bool {
        true
    }
    #[inline]
    fn charge(&mut self, _bytes: u64) {}
    #[inline]
    fn release(&mut self, _bytes: u64) {}
}

/// A budget of a fixed number of bytes, which the owner sets from the memory it gives its
/// replicas.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Bytes {
    limit: u64,
    used: u64,
}

impl Bytes {
    /// A budget of `limit` bytes, none of them used.
    pub const fn new(limit: u64) -> Self {
        Self { limit, used: 0 }
    }

    /// The bytes reserved and charged now.
    pub const fn used(&self) -> u64 {
        self.used
    }

    /// The budget's bytes.
    pub const fn limit(&self) -> u64 {
        self.limit
    }
}

impl Budget for Bytes {
    /// Counts every byte reserved and charged.
    const BOUNDED: bool = true;
    fn reserve(&mut self, bytes: u64) -> bool {
        match self.used.checked_add(bytes) {
            Some(used) if used <= self.limit => {
                self.used = used;
                true
            }
            _ => false,
        }
    }
    fn charge(&mut self, bytes: u64) {
        // Saturating: a charge past what u64 counts is past every limit, which refuses every
        // reservation after it, as the true charge would.
        self.used = self.used.saturating_add(bytes);
    }
    fn release(&mut self, bytes: u64) {
        // Saturating: a release past what was used is a release of everything.
        self.used = self.used.saturating_sub(bytes);
    }
}
