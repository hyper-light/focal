//! The unwind boundary around the caller's code a crate runs: wakers, closures, futures, files,
//! admissions (CLAUDE.md §1: production code never panics, and a dependency or caller's code that
//! can is called behind an unwind boundary).

use std::panic::{AssertUnwindSafe, catch_unwind};

/// Runs `op`, the caller's code, inside an unwind boundary: `None` if it unwound. The unwind's
/// payload is the caller's too, so it is dropped inside a boundary of its own; a payload whose drop
/// unwinds as well leaves that second payload unfreed, one allocation, rather than unwind (std's
/// own thread result aborts the process there).
pub(crate) fn contained<R>(op: impl FnOnce() -> R) -> Option<R> {
    match catch_unwind(AssertUnwindSafe(op)) {
        Ok(value) => Some(value),
        Err(payload) => {
            if let Err(again) = catch_unwind(AssertUnwindSafe(move || drop(payload))) {
                let _ = Box::leak(again);
            }
            None
        }
    }
}
