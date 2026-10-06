//! Device nodes opened directly: a disk or partition written with no file system between.
//!
//! stat(2) gives a node no size, so a node's length is the capacity its device reports:
//! lseek(2) to the end on Linux, whose block device files answer with the device's size
//! (block/fops.c, `blkdev_llseek`); the disk ioctls on macOS; IOCTL_DISK_GET_LENGTH_INFO on
//! Windows. A node's full flush is the platform's own where that reaches the device:
//! fdatasync(2) of a Linux block device writes its cached pages and then flushes the
//! device's cache (block/fops.c, `blkdev_fsync`), and FlushFileBuffers takes a disk or
//! volume handle on Windows. macOS refuses F_FULLFSYNC on a node, so its flush there is the
//! device's own (`macos`).

use std::fs::File;
use std::io;
use std::path::Path;

#[cfg(target_vendor = "apple")]
mod macos;
#[cfg(windows)]
mod windows;

#[cfg(target_vendor = "apple")]
pub(crate) use macos::{len, sync};
#[cfg(windows)]
pub(crate) use windows::{len, storage_info};

/// Whether `file`, opened at `path`, is a device node rather than a regular file.
#[cfg(unix)]
pub(crate) fn is_node(file: &File, _path: &Path) -> io::Result<bool> {
    use std::os::unix::fs::FileTypeExt;
    let kind = file.metadata()?.file_type();
    Ok(kind.is_block_device() || kind.is_char_device())
}

/// Windows names devices in its device namespace: a path beginning `\\.\` "will access the
/// Win32 device namespace instead of the Win32 file namespace" (Naming Files, Paths, and
/// Namespaces).
#[cfg(windows)]
pub(crate) fn is_node(_file: &File, path: &Path) -> io::Result<bool> {
    Ok(path.as_os_str().to_string_lossy().starts_with(r"\\.\"))
}

#[cfg(any(target_os = "linux", target_os = "android"))]
pub(crate) fn len(file: &File) -> io::Result<u64> {
    use std::io::Seek;
    // Every transfer names its offset, so moving the file's position changes nothing.
    let mut end = file;
    end.seek(io::SeekFrom::End(0))
}

#[cfg(any(target_os = "linux", target_os = "android", windows))]
pub(crate) fn sync(file: &File) -> io::Result<()> {
    file.sync_data()
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_vendor = "apple",
    windows
)))]
pub(crate) fn len(_file: &File) -> io::Result<u64> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "no device size for this operating system",
    ))
}

/// Without a flush known to reach the device, a node takes no acknowledged write.
#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_vendor = "apple",
    windows
)))]
pub(crate) fn sync(_file: &File) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "no device flush for this operating system",
    ))
}
