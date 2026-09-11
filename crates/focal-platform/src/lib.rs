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
//! The one place platform-specific filesystem behaviour lives (decisions 10
//! and 11, docs 03 §12, 20): advisory file locks on the stable
//! `std::fs::File` primitives (replacing `fs2`), the free space of a volume,
//! and — on Windows — protected files and directories behind a single
//! audited FFI file. Business logic uses these types, never a platform call
//! of its own; `scripts/check-contracts.py` enforces that the `unsafe` token
//! appears nowhere else under `crates/`.
//!
//! Locks are advisory and process-scoped, released when the file's last
//! descriptor closes; a lock owner is `!Clone` so a critical section has one
//! release authority ([focal-client](../../focal-client/src/file_lock.rs)
//! documents why).
use std::{fs::File, io};

/// Take an exclusive whole-file lock without blocking. A lock another owner
/// holds returns `io::ErrorKind::WouldBlock`, mapped from the standard
/// library's `TryLockError` so callers keep their existing classification.
/// Free functions, not an extension trait, so they never collide with the
/// standard library's inherent `File` lock methods.
pub fn try_lock_exclusive(file: &File) -> io::Result<()> {
    map_try(File::try_lock(file))
}
/// Take a shared whole-file lock without blocking; `WouldBlock` when an
/// exclusive lock is held.
pub fn try_lock_shared(file: &File) -> io::Result<()> {
    map_try(File::try_lock_shared(file))
}
/// Release `file`'s lock.
pub fn unlock(file: &File) -> io::Result<()> {
    File::unlock(file)
}
fn map_try(result: Result<(), std::fs::TryLockError>) -> io::Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(std::fs::TryLockError::WouldBlock) => Err(io::Error::from(io::ErrorKind::WouldBlock)),
        Err(std::fs::TryLockError::Error(error)) => Err(error),
    }
}

/// Bytes free on the volume that holds `path`, or `None` when it cannot be
/// determined (the caller treats an unknown free space as no headroom).
#[cfg(unix)]
pub fn available_space(path: &std::path::Path) -> Option<u64> {
    // rustix wraps `statvfs` safely; no `unsafe` on the Unix path.
    let stat = rustix::fs::statvfs(path).ok()?;
    stat.f_bavail.checked_mul(stat.f_frsize)
}

#[cfg(windows)]
pub fn available_space(path: &std::path::Path) -> Option<u64> {
    windows::available_space(path)
}

#[cfg(not(any(unix, windows)))]
pub fn available_space(_path: &std::path::Path) -> Option<u64> {
    None
}

pub mod fs;
#[cfg(windows)]
mod windows;

/// The authenticated local-transport named-pipe primitives (Windows only):
/// an owner-only pipe server and same-user peer verification. The
/// [focal-wire](../../focal-wire/src/local/windows.rs) local transport is the
/// only caller; the `unsafe` FFI stays confined to [windows].
#[cfg(windows)]
pub use tokio::net::windows::named_pipe::{NamedPipeClient, NamedPipeServer};
#[cfg(windows)]
pub use windows::{create_pipe_server, pipe_client_is_current_owner, pipe_server_is_current_owner};

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::fs::File;

    #[test]
    fn an_exclusive_lock_excludes_a_second_owner_until_released() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("LOCK");
        let owner = File::create(&path).unwrap();
        try_lock_exclusive(&owner).unwrap();
        let other = File::options().read(true).write(true).open(&path).unwrap();
        assert_eq!(
            try_lock_exclusive(&other).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        unlock(&owner).unwrap();
        try_lock_exclusive(&other).unwrap();
    }

    #[test]
    fn available_space_reports_a_positive_figure_for_a_real_directory() {
        let dir = tempfile::tempdir().unwrap();
        assert!(available_space(dir.path()).is_some_and(|free| free > 0));
    }
}
