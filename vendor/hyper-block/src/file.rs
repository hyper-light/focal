//! A file on a local device, opened for direct I/O where the platform and file system accept
//! it, with positional reads and writes and the platform's exact durability flush.
//!
//! Direct I/O per platform: Linux `O_DIRECT` (open(2)), macOS `F_NOCACHE` (fcntl(2)), Windows
//! `FILE_FLAG_NO_BUFFERING` (CreateFileW, "File Buffering"). A file system that refuses it is
//! opened buffered and says so in [`DeviceFile::caching`].
//!
//! Durability per platform is what `std::fs::File::sync_data` issues, verified in the std
//! source (library/std/src/sys/fs/{unix,windows}.rs): `fdatasync(2)` on Linux,
//! `fcntl(F_FULLFSYNC)` on macOS (plain fsync(2) there leaves data in the drive's volatile
//! cache, per Apple's fsync(2) man page), and `FlushFileBuffers` on Windows.
//!
//! The path may also name a device node, a disk or partition written directly: its length
//! and full flush are then the device's (`crate::node`). A device that refuses writes out of
//! a zone's order is refused at open, before any write (`refuse_zones`).

use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

use crate::DiskError;
use crate::buf::Alignment;

/// Whether transfers bypass the OS page cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Caching {
    /// Transfers go straight between the device and the buffer.
    Direct,
    /// Transfers go through the OS page cache.
    Buffered,
}

/// What the caller asks for when opening.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CachingRequest {
    /// Direct I/O if the file system accepts it, otherwise buffered.
    PreferDirect,
    /// Through the OS page cache.
    Buffered,
}

/// A file, or a device node, read and written at offsets with the platform's full flush.
#[derive(Debug)]
pub struct DeviceFile {
    file: File,
    path: PathBuf,
    caching: Caching,
    align: Alignment,
    /// A device node rather than a regular file.
    node: bool,
}

impl DeviceFile {
    /// Opens `path` for reading and writing, creating it when `create` is set.
    ///
    /// `align` is the offset and length alignment direct transfers need on this file (the
    /// device's logical block size, or what statx(STATX_DIOALIGN) reports); it is enforced on
    /// every transfer while the file is direct.
    pub fn open(
        path: &Path,
        create: bool,
        request: CachingRequest,
        align: Alignment,
    ) -> Result<Self, DiskError> {
        let wrap = |op: &'static str| {
            let path = path.to_path_buf();
            move |source| DiskError::Io { op, path, source }
        };
        let (file, caching, align) = match request {
            CachingRequest::PreferDirect => match sys::open_direct(path, create) {
                Ok(file) => (file, Caching::Direct, align),
                Err(e) if sys::refuses_direct(&e) => (
                    options(create).open(path).map_err(wrap("open"))?,
                    Caching::Buffered,
                    Alignment::BYTE,
                ),
                Err(e) => return Err(wrap("open")(e)),
            },
            CachingRequest::Buffered => (
                options(create).open(path).map_err(wrap("open"))?,
                Caching::Buffered,
                Alignment::BYTE,
            ),
        };
        let node = crate::node::is_node(&file, path).map_err(wrap("stat"))?;
        refuse_zones(path, &file, node)?;
        Ok(Self {
            file,
            path: path.to_path_buf(),
            caching,
            align,
            node,
        })
    }

    /// The path the file was opened at.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// A second handle to the same open file (`dup` on Unix, `DuplicateHandle` on Windows,
    /// through `File::try_clone`): it shares the open file description, so its direct-I/O
    /// setting, and its positional writes and flushes are the file's own.
    pub fn try_clone(&self) -> Result<Self, DiskError> {
        let file = self.file.try_clone().map_err(|source| DiskError::Io {
            op: "duplicate",
            path: self.path.clone(),
            source,
        })?;
        Ok(Self {
            file,
            path: self.path.clone(),
            caching: self.caching,
            align: self.align,
            node: self.node,
        })
    }

    /// Whether the file's transfers bypass the page cache.
    pub fn caching(&self) -> Caching {
        self.caching
    }

    /// The alignment every transfer's offset and length must meet (one byte when buffered).
    pub fn alignment(&self) -> Alignment {
        self.align
    }

    /// Writes all of `buf` at `offset`.
    pub fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
        self.check_alignment(offset, buf.len())?;
        self.check_address(buf.as_ptr().addr(), offset, buf.len())?;
        let mut done = 0usize;
        while let Some(rest) = buf.get(done..).filter(|r| !r.is_empty()) {
            let at = offset
                .checked_add(u64::try_from(done).map_err(|_| self.overflow(offset))?)
                .ok_or_else(|| self.overflow(offset))?;
            match sys::write_at(&self.file, rest, at) {
                Ok(0) => {
                    return Err(self.io_error("write", io::Error::from(io::ErrorKind::WriteZero)));
                }
                Ok(n) => done = done.saturating_add(n),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(self.io_error("write", e)),
            }
        }
        Ok(())
    }

    /// Fills `buf` from `offset`; reaching the end of the file first is an error.
    pub fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
        let filled = self.read_at(buf, offset)?;
        if filled < buf.len() {
            return Err(DiskError::ShortRead {
                path: self.path.clone(),
                offset,
                missing: buf.len().saturating_sub(filled),
            });
        }
        Ok(())
    }

    /// Reads into `buf` from `offset` until it is full or the file ends; returns the count.
    pub fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize, DiskError> {
        self.check_alignment(offset, buf.len())?;
        self.check_address(buf.as_ptr().addr(), offset, buf.len())?;
        let mut done = 0usize;
        while let Some(rest) = buf.get_mut(done..).filter(|r| !r.is_empty()) {
            let at = offset
                .checked_add(u64::try_from(done).map_err(|_| self.overflow(offset))?)
                .ok_or_else(|| self.overflow(offset))?;
            match sys::read_at(&self.file, rest, at) {
                Ok(0) => break,
                Ok(n) => done = done.saturating_add(n),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(self.io_error("read", e)),
            }
        }
        Ok(done)
    }

    /// Makes every completed write durable, including the device's volatile cache.
    pub fn sync_data(&self) -> Result<(), DiskError> {
        let synced = if self.node {
            crate::node::sync(&self.file)
        } else {
            self.file.sync_data()
        };
        synced.map_err(|e| self.io_error("sync_data", e))
    }

    /// Reserves space so the file is `len` bytes long with its blocks allocated. Only for a
    /// file that is still empty (see `sys::preallocate`); a device node's space is the
    /// device's, and is not allocated.
    pub fn preallocate(&self, len: u64) -> Result<(), DiskError> {
        if self.node {
            return Err(DiskError::Unsupported {
                path: self.path.clone(),
                reason: "a device node is not allocated: its space is the device's",
            });
        }
        sys::preallocate(&self.file, len).map_err(|e| self.io_error("preallocate", e))
    }

    /// The file's length, or a device node's capacity.
    pub fn len(&self) -> Result<u64, DiskError> {
        if self.node {
            return crate::node::len(&self.file).map_err(|e| self.io_error("device size", e));
        }
        self.file
            .metadata()
            .map(|m| m.len())
            .map_err(|e| self.io_error("stat", e))
    }

    /// Whether the file holds no bytes.
    pub fn is_empty(&self) -> Result<bool, DiskError> {
        self.len().map(|len| len == 0)
    }

    /// The open file.
    pub fn std_file(&self) -> &File {
        &self.file
    }

    fn check_alignment(&self, offset: u64, len: usize) -> Result<(), DiskError> {
        if self.align.is_aligned_u64(offset) && self.align.is_aligned(len) {
            return Ok(());
        }
        Err(DiskError::Misaligned {
            offset,
            len,
            align: self.align.get(),
        })
    }

    /// Direct transfers move straight between the device and the buffer, so its address
    /// must be aligned as well (open(2) NOTES; Windows "File Buffering"). A transfer of no
    /// bytes moves none and reaches no system call: an empty buffer, which allocates nothing,
    /// has no first byte to align, and its address is the empty slice's, one.
    fn check_address(&self, addr: usize, offset: u64, len: usize) -> Result<(), DiskError> {
        if len == 0 || self.align.is_aligned(addr) {
            return Ok(());
        }
        Err(DiskError::Misaligned {
            offset,
            len,
            align: self.align.get(),
        })
    }

    fn io_error(&self, op: &'static str, source: io::Error) -> DiskError {
        DiskError::Io {
            op,
            path: self.path.clone(),
            source,
        }
    }

    fn overflow(&self, offset: u64) -> DiskError {
        self.io_error(
            "offset",
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("transfer at {offset} passes the largest file offset"),
            ),
        )
    }
}

/// Refuses what cannot hold a `DeviceFile` (mantle audit B10). A log or a volume writes
/// anywhere in its file: superblocks, a circular index log and reused segments all rewrite
/// earlier offsets. A host-managed zoned device refuses a write anywhere but at its zone's write
/// pointer (ZBC/ZAC, as Linux's zonefs documentation summarizes), and every zonefs file is a
/// zone. A file on a file system over such a device is placed by that file system.
fn refuse_zones(path: &Path, file: &File, node: bool) -> Result<(), DiskError> {
    match zone_refusal(zones::of(path, file, node), node) {
        Some(reason) => Err(DiskError::Unsupported {
            path: path.to_path_buf(),
            reason,
        }),
        None => Ok(()),
    }
}

/// What the zone check knows of the storage under a path.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Zones {
    /// The file is a zonefs file.
    zonefs: bool,
    /// The device takes writes only in each zone's order.
    host_managed: bool,
}

fn zone_refusal(zones: Zones, node: bool) -> Option<&'static str> {
    if node && zones.host_managed {
        Some(
            "a host-managed zoned device takes writes only in each zone's order, and there is \
             no zone backend",
        )
    } else if zones.zonefs {
        Some(
            "a zonefs file is a zone, written only at its write pointer, and there is no zone \
             backend",
        )
    } else {
        None
    }
}

/// Linux names both facts: statfs(2)'s `f_type` is zonefs's `ZONEFS_MAGIC` (0x5a4f4653,
/// include/uapi/linux/magic.h) on a zonefs file, and a block device's `queue/zoned` reads
/// `host-managed` for a device that takes writes only in zone order (Documentation/ABI/stable/
/// sysfs-block); a partition's queue is its disk's, one directory up.
#[cfg(any(target_os = "linux", target_os = "android"))]
mod zones {
    use std::fs::File;
    use std::path::Path;

    use super::Zones;

    /// zonefs's `f_type` (include/uapi/linux/magic.h, `ZONEFS_MAGIC`).
    const ZONEFS_MAGIC: i128 = 0x5A4F_4653;

    pub(super) fn of(path: &Path, file: &File, node: bool) -> Zones {
        let zonefs = rustix::fs::statfs(path).is_ok_and(|s| i128::from(s.f_type) == ZONEFS_MAGIC);
        let host_managed = node && host_managed(file);
        Zones {
            zonefs,
            host_managed,
        }
    }

    fn host_managed(file: &File) -> bool {
        let Ok(stat) = rustix::fs::fstat(file) else {
            return false;
        };
        let (major, minor) = (
            rustix::fs::major(stat.st_rdev),
            rustix::fs::minor(stat.st_rdev),
        );
        let device = format!("/sys/dev/block/{major}:{minor}");
        [
            format!("{device}/queue/zoned"),
            format!("{device}/../queue/zoned"),
        ]
        .iter()
        .find_map(|p| std::fs::read_to_string(p).ok())
        .is_some_and(|zoned| zoned.trim() == "host-managed")
    }
}

/// macOS and Windows have no zoned block devices this code can be given: neither exposes
/// host-managed zones to a file system or to a disk's node.
#[cfg(not(any(target_os = "linux", target_os = "android")))]
mod zones {
    use std::fs::File;
    use std::path::Path;

    use super::Zones;

    pub(super) fn of(_path: &Path, _file: &File, _node: bool) -> Zones {
        Zones::default()
    }
}

/// The size the operating system reports as the one to write `file` in, opened at `path`:
/// `st_blksize` on Unix (stat(2), "the preferred block size for efficient filesystem I/O"), and on
/// Windows the physical sector size the file's volume performs best at
/// (`GetFileInformationByHandleEx`'s `FILE_STORAGE_INFO::PhysicalBytesPerSectorForPerformance`).
/// A write of it, at an offset it divides, is one the device takes whole, never read, merged and
/// written back. A size of zero is refused as corrupt.
pub fn preferred_block(file: &File, path: &Path) -> Result<usize, DiskError> {
    let wrap = |source| DiskError::Io {
        op: "preferred_block",
        path: path.to_path_buf(),
        source,
    };
    let bytes = sys::preferred_block(file).map_err(wrap)?;
    if bytes == 0 {
        return Err(DiskError::Corrupt {
            path: path.to_path_buf(),
            what: "a file whose preferred block size the system gives as zero",
        });
    }
    Ok(bytes)
}

/// Flushes a directory so entries created or renamed in it survive a crash (Pillai et al.,
/// OSDI 2014: a new file's directory entry is durable only after its directory is synced).
pub fn sync_dir(dir: &Path) -> Result<(), DiskError> {
    sys::sync_dir(dir).map_err(|source| DiskError::Io {
        op: "sync_dir",
        path: dir.to_path_buf(),
        source,
    })
}

fn options(create: bool) -> OpenOptions {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(create);
    options
}

#[cfg(unix)]
mod sys {
    use std::fs::File;
    use std::io;
    use std::os::unix::fs::FileExt;
    use std::path::Path;

    pub(super) fn write_at(file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
        file.write_at(buf, offset)
    }

    pub(super) fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        file.read_at(buf, offset)
    }

    pub(super) fn sync_dir(dir: &Path) -> io::Result<()> {
        File::open(dir)?.sync_all()
    }

    pub(super) fn preferred_block(file: &File) -> io::Result<usize> {
        use std::os::unix::fs::MetadataExt;
        usize::try_from(file.metadata()?.blksize()).map_err(|_| io::ErrorKind::InvalidData.into())
    }

    pub(super) fn preallocate(file: &File, len: u64) -> io::Result<()> {
        // Linux: fallocate(2) mode 0 allocates and extends. macOS: rustix issues
        // F_PREALLOCATE from the physical end of file, then ftruncate(2) — correct only
        // for an empty file, which is the one use.
        rustix::fs::fallocate(file, rustix::fs::FallocateFlags::empty(), 0, len)
            .map_err(io::Error::from)
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub(super) fn open_direct(path: &Path, create: bool) -> io::Result<File> {
        use std::os::unix::fs::OpenOptionsExt;
        let flag = i32::try_from(rustix::fs::OFlags::DIRECT.bits())
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
        let mut options = super::options(create);
        options.custom_flags(flag);
        options.open(path)
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub(super) fn refuses_direct(e: &io::Error) -> bool {
        // open(2): EINVAL "The filesystem does not support the O_DIRECT flag".
        e.raw_os_error() == Some(rustix::io::Errno::INVAL.raw_os_error())
    }

    #[cfg(target_vendor = "apple")]
    pub(super) fn open_direct(path: &Path, create: bool) -> io::Result<File> {
        let file = super::options(create).open(path)?;
        rustix::fs::fcntl_nocache(&file, true)?;
        Ok(file)
    }

    #[cfg(target_vendor = "apple")]
    pub(super) fn refuses_direct(e: &io::Error) -> bool {
        matches!(
            e.raw_os_error(),
            Some(code) if code == rustix::io::Errno::INVAL.raw_os_error()
                || code == rustix::io::Errno::NOTSUP.raw_os_error()
        )
    }

    #[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
    pub(super) fn open_direct(_path: &Path, _create: bool) -> io::Result<File> {
        Err(io::Error::from(io::ErrorKind::Unsupported))
    }

    #[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
    pub(super) fn refuses_direct(e: &io::Error) -> bool {
        e.kind() == io::ErrorKind::Unsupported
    }
}

#[cfg(windows)]
mod sys {
    use std::fs::File;
    use std::io;
    use std::os::windows::fs::{FileExt, OpenOptionsExt};
    use std::path::Path;

    use windows_sys::Win32::Foundation::ERROR_INVALID_PARAMETER;
    use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_NO_BUFFERING;

    pub(super) fn write_at(file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
        file.seek_write(buf, offset)
    }

    pub(super) fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        // std maps ReadFile's ERROR_HANDLE_EOF to Ok(0) (library/std/src/sys/pal/windows/handle.rs).
        file.seek_read(buf, offset)
    }

    pub(super) fn sync_dir(dir: &Path) -> io::Result<()> {
        // A directory opens only with FILE_FLAG_BACKUP_SEMANTICS (CreateFileW); FlushFileBuffers
        // needs a handle with write access.
        use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS;
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
            .open(dir)?
            .sync_all()
    }

    pub(super) fn preferred_block(file: &File) -> io::Result<usize> {
        let sector = crate::node::storage_info(file)?.PhysicalBytesPerSectorForPerformance;
        usize::try_from(sector).map_err(|_| io::ErrorKind::InvalidData.into())
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "the device layer sizes the files it owns; preallocation is its job"
    )]
    pub(super) fn preallocate(file: &File, len: u64) -> io::Result<()> {
        // SetEndOfFile allocates the clusters of a non-sparse NTFS file; the valid data
        // length stays at zero, so reads past what was written return zeros.
        file.set_len(len)
    }

    pub(super) fn open_direct(path: &Path, create: bool) -> io::Result<File> {
        let mut options = super::options(create);
        options.custom_flags(FILE_FLAG_NO_BUFFERING);
        options.open(path)
    }

    pub(super) fn refuses_direct(e: &io::Error) -> bool {
        e.raw_os_error() == i32::try_from(ERROR_INVALID_PARAMETER).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buf::AlignedBuf;

    fn align() -> Alignment {
        Alignment::new(4096).unwrap()
    }

    /// A transfer of no bytes from an empty buffer, whose address is the empty slice's, is no
    /// misaligned transfer: it moves nothing at every alignment. proptest drew a capacity of
    /// zero only now and then, and windows-2025 found the address check refusing it.
    #[test]
    fn an_empty_buffer_transfers_nothing_at_every_alignment() {
        let dir = tempfile::tempdir().unwrap();
        // Direct where the file system takes it (a buffered file is aligned to a byte).
        let file = DeviceFile::open(
            &dir.path().join("empty"),
            true,
            CachingRequest::PreferDirect,
            align(),
        )
        .unwrap();
        for shift in 0..=crate::buf::MAX_ALIGNMENT.trailing_zeros() {
            let mut empty = AlignedBuf::zeroed(0, Alignment::new(1 << shift).unwrap()).unwrap();
            file.write_all_at(empty.as_slice(), 0).unwrap();
            file.read_exact_at(empty.as_mut_capacity(), 0).unwrap();
            assert_eq!(file.read_at(empty.as_mut_capacity(), 0).unwrap(), 0);
        }
    }

    /// The size the system reports for a file is a power of two no smaller than the smallest
    /// sector any device has (512 bytes, the logical sector of every ATA and SCSI disk), and a
    /// write and flush of it at the start of the file goes through.
    #[test]
    fn a_file_is_written_in_the_block_its_system_reports() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("block");
        let file = options(true).open(&path).unwrap();
        let block = preferred_block(&file, &path).unwrap();
        assert!(block.is_power_of_two() && block >= 512, "{block}");
        sys::write_at(&file, &vec![0xa5; block], 0).unwrap();
        file.sync_data().unwrap();
        assert_eq!(file.metadata().unwrap().len(), block as u64);
    }

    /// A host-managed device node and a zonefs file are refused; a file on a file system
    /// over a zoned device, and a host-aware device, which takes writes anywhere, are not
    /// (mantle audit B10).
    #[test]
    fn storage_that_takes_writes_only_in_zone_order_is_refused() {
        let refused = |zonefs, host_managed, node| {
            zone_refusal(
                Zones {
                    zonefs,
                    host_managed,
                },
                node,
            )
            .is_some()
        };
        assert!(refused(false, true, true));
        assert!(refused(true, true, false));
        assert!(!refused(false, true, false));
        assert!(!refused(false, false, true));
        let dir = tempfile::tempdir().unwrap();
        let file = DeviceFile::open(
            &dir.path().join("plain"),
            true,
            CachingRequest::Buffered,
            align(),
        );
        assert!(file.is_ok(), "a file on an ordinary file system is taken");
    }

    /// A device node's length is its device's capacity, it takes aligned writes and reads
    /// like a file, and its flush reaches the device (macOS: a disk image's node, where
    /// F_FULLFSYNC fails and the length of a node reads zero).
    #[cfg(target_vendor = "apple")]
    #[test]
    fn a_device_node_has_its_devices_length_and_flush() {
        let dir = tempfile::tempdir().unwrap();
        let image = crate::image::DiskImage::attach(dir.path(), 16).unwrap();
        let file =
            DeviceFile::open(image.node(), false, CachingRequest::PreferDirect, align()).unwrap();
        assert_eq!(file.len().unwrap(), 16 << 20);
        let pattern: Vec<u8> = (0..8192u32).map(|i| (i % 251) as u8).collect();
        let mut out = AlignedBuf::zeroed(8192, align()).unwrap();
        out.extend_from_slice(&pattern).unwrap();
        file.write_all_at(out.as_slice(), 1 << 20).unwrap();
        file.sync_data().unwrap();
        let mut back = AlignedBuf::zeroed(8192, align()).unwrap();
        file.read_exact_at(back.as_mut_capacity(), 1 << 20).unwrap();
        back.set_len(8192).unwrap();
        assert_eq!(back.as_slice(), pattern.as_slice());
        assert!(matches!(
            file.preallocate(1 << 20),
            Err(DiskError::Unsupported { .. })
        ));
    }

    #[test]
    fn direct_round_trip_is_durable_and_exact() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("volume");
        let file = DeviceFile::open(&path, true, CachingRequest::PreferDirect, align()).unwrap();
        file.preallocate(1 << 20).unwrap();
        assert_eq!(file.len().unwrap(), 1 << 20);

        let mut out = AlignedBuf::zeroed(8192, align()).unwrap();
        let pattern: Vec<u8> = (0..8192u32).map(|i| (i % 251) as u8).collect();
        out.extend_from_slice(&pattern).unwrap();
        file.write_all_at(out.as_slice(), 4096).unwrap();
        file.sync_data().unwrap();
        sync_dir(dir.path()).unwrap();

        let mut back = AlignedBuf::zeroed(8192, align()).unwrap();
        file.read_exact_at(back.as_mut_capacity(), 4096).unwrap();
        back.set_len(8192).unwrap();
        assert_eq!(back.as_slice(), pattern.as_slice());
    }

    #[test]
    fn direct_transfers_must_be_aligned() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("volume");
        let file = DeviceFile::open(&path, true, CachingRequest::PreferDirect, align()).unwrap();
        if file.caching() == Caching::Buffered {
            return;
        }
        let buf = AlignedBuf::zeroed(4096, align()).unwrap();
        let err = file.write_all_at(&buf.as_slice()[..0], 100).unwrap_err();
        assert!(matches!(err, DiskError::Misaligned { offset: 100, .. }));
        let mut small = [0u8; 100];
        let err = file.read_at(&mut small, 0).unwrap_err();
        assert!(matches!(err, DiskError::Misaligned { len: 100, .. }));
    }

    #[test]
    fn buffered_accepts_any_offset_and_reports_short_reads() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        let file = DeviceFile::open(&path, true, CachingRequest::Buffered, align()).unwrap();
        assert_eq!(file.caching(), Caching::Buffered);
        file.write_all_at(b"hello", 3).unwrap();
        let mut buf = [0u8; 8];
        file.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"\0\0\0hello");
        let err = file.read_exact_at(&mut buf, 4).unwrap_err();
        assert!(matches!(err, DiskError::ShortRead { missing: 4, .. }));
    }

    #[test]
    fn opening_a_missing_file_without_create_fails_with_its_path() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("absent");
        let err =
            DeviceFile::open(&path, false, CachingRequest::PreferDirect, align()).unwrap_err();
        match err {
            DiskError::Io {
                op,
                path: p,
                source,
            } => {
                assert_eq!(op, "open");
                assert_eq!(p, path);
                assert_eq!(source.kind(), io::ErrorKind::NotFound);
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}
