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

/// A path's bytes in a form that round-trips back through [path_from_bytes] on
/// the same platform: the operating system's own byte encoding on Unix, and
/// UTF-16LE on Windows (where a path is a sequence of 16-bit code units).
/// Callers journal a path the OS handed them and rebuild it verbatim later;
/// the bytes are opaque and are never interpreted across platforms. Both
/// directions are safe standard-library conversions — no `unsafe`, no FFI.
pub fn path_to_bytes(path: &std::path::Path) -> Vec<u8> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        path.as_os_str().as_bytes().to_vec()
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        path.as_os_str()
            .encode_wide()
            .flat_map(u16::to_le_bytes)
            .collect()
    }
    #[cfg(not(any(unix, windows)))]
    {
        path.as_os_str().as_encoded_bytes().to_vec()
    }
}

/// Rebuild a path from bytes [path_to_bytes] produced on this platform.
/// Returns `None` when the bytes cannot name a path here: they contain an
/// interior NUL (invalid in every path), or, on Windows, they are not a whole
/// number of 16-bit code units.
pub fn path_from_bytes(bytes: &[u8]) -> Option<std::path::PathBuf> {
    #[cfg(unix)]
    {
        if bytes.contains(&0) {
            return None;
        }
        use std::os::unix::ffi::OsStringExt;
        Some(std::ffi::OsString::from_vec(bytes.to_vec()).into())
    }
    #[cfg(windows)]
    {
        if bytes.len() % 2 != 0 {
            return None;
        }
        use std::os::windows::ffi::OsStringExt;
        let wide: Vec<u16> = bytes
            .chunks_exact(2)
            .filter_map(|pair| <[u8; 2]>::try_from(pair).ok().map(u16::from_le_bytes))
            .collect();
        if wide.contains(&0) {
            return None;
        }
        Some(std::ffi::OsString::from_wide(&wide).into())
    }
    #[cfg(not(any(unix, windows)))]
    {
        if bytes.contains(&0) {
            return None;
        }
        std::str::from_utf8(bytes)
            .ok()
            .map(std::path::PathBuf::from)
    }
}

/// Make a directory entry (a create, rename or delete within it) durable. On
/// Unix this opens the directory and fsyncs it. On Windows a directory handle
/// cannot be flushed (opening one for `sync_all` fails with access-denied);
/// durability there comes from write-through file writes and `MoveFileExW`
/// (`atomic_replace`), so this is a no-op (decision 10, doc 20 §3).
pub fn sync_dir(path: &std::path::Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        File::open(path)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;

    #[test]
    fn path_bytes_round_trip_on_this_platform() {
        let path = std::path::Path::new("some/dir/with a space/leaf.bin");
        let bytes = path_to_bytes(path);
        assert_eq!(path_from_bytes(&bytes).as_deref(), Some(path));
    }

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
