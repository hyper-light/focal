//! The memory structures the runtime is built on, from slates' `mem` (ORIGIN.md): the packed task word
//! and generational handles, and the bounds every loom model explores under. slates' slab, segmented
//! storage and rings went with the redesign (docs/runtime.md §3.2, §3.4): the desk's `Cell` structures and
//! the wake bitmap replaced them.

pub mod error;
pub mod handle;
#[cfg(loom)]
pub mod loom_bounds;

pub use error::MemError;
pub use handle::{Encoded, Handle};
