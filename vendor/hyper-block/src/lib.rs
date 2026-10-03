//! Block I/O for the shared log (mantle note 32 §3.9): how to move bytes to a device with the
//! alignment and durability the device and OS actually guarantee, from mantle-disk at mantle
//! `147f035` (`ORIGIN.md`).
//!
//! Identifying and measuring a device stays in mantle-disk: a caller that knows the device's
//! alignment, queue and measured depth hands them in.

#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros,
        clippy::cognitive_complexity,
        clippy::cast_possible_truncation
    )
)]

use std::path::PathBuf;

pub mod block;
pub mod buf;
pub mod commit;
pub mod file;
#[cfg(all(target_vendor = "apple", any(test, feature = "sim")))]
pub mod image;
pub mod issuer;
mod node;
pub mod record;
pub mod scratch;
#[cfg(any(test, feature = "sim"))]
pub mod sim;
pub mod threads;

/// What a device operation fails with.
#[derive(Debug, thiserror::Error)]
pub enum DiskError {
    /// The OS refused an operation on a file or device.
    #[error("{op} {}: {source}", path.display())]
    Io {
        /// What was being done.
        op: &'static str,
        /// The file or device it was done to.
        path: PathBuf,
        /// The OS's error.
        #[source]
        source: std::io::Error,
    },
    /// A direct transfer whose offset, length or buffer does not meet the file's alignment.
    #[error("direct transfer at offset {offset} of {len} bytes is not aligned to {align}")]
    Misaligned {
        /// The transfer's file offset.
        offset: u64,
        /// Its length in bytes.
        len: usize,
        /// The alignment it had to meet.
        align: usize,
    },
    /// A read reached the end of the file before it was full.
    #[error("{} ended at offset {offset}: {missing} bytes short", path.display())]
    ShortRead {
        /// The file read.
        path: PathBuf,
        /// The read's file offset.
        offset: u64,
        /// Bytes the file did not have.
        missing: usize,
    },
    /// A buffer refused a size or an alignment.
    #[error(transparent)]
    Buf(#[from] buf::BufError),
    /// A pool for `path` would take more threads than the process budget has left; refused
    /// before any thread starts (docs/design/node.md §1.2).
    #[error(
        "{}: {asked} threads asked of a process budget with {left} of {ceiling} left",
        path.display()
    )]
    Threads {
        /// The device the pool would serve.
        path: PathBuf,
        /// Threads the pool asked for.
        asked: usize,
        /// Threads the budget had left.
        left: usize,
        /// The budget's ceiling.
        ceiling: usize,
    },
    /// Storage that cannot be written as it must be, refused before any write.
    #[error("{}: {reason}", path.display())]
    Unsupported {
        /// The file or device refused.
        path: PathBuf,
        /// Why.
        reason: &'static str,
    },
    /// A record that does not hold what was written: its checksum fails, or it is longer than its
    /// reader's bound (`record`).
    #[error("{}: {what}", path.display())]
    Corrupt {
        /// The record.
        path: PathBuf,
        /// What does not hold.
        what: &'static str,
    },
}
