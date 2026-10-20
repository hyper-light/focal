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
    /// The block a writer lays the file out in ([`DeviceFile::layout_block`]).
    layout: Alignment,
    /// A device node rather than a regular file.
    node: bool,
    /// What this handle needs to read from memory alone ([`DeviceFile::read_resident_at`]).
    resident: crate::resident::Resident,
    /// The epoch its kept answers hold in, unless a caller reading through several files
    /// passes its own ([`DeviceFile::read_resident_with`]).
    epoch: crate::resident::Epoch,
    /// Whether new space is written with zeros before use ([`DeviceFile::fills_new_space`]),
    /// asked of the file system once, at open.
    fills: bool,
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
        // A file still empty is asked to be written in place where its file system would copy it
        // on write (`sys::written_in_place`): btrfs takes the mark only now, before the file's
        // first byte, and a log's file comes here empty whoever made it (a caller may make it
        // first, and open it without `create`), and grows by writes, never preallocated.
        if !node && file.metadata().map_err(wrap("stat"))?.len() == 0 {
            sys::written_in_place(&file);
        }
        let layout = match caching {
            Caching::Direct => align,
            Caching::Buffered => layout_of(&file, path, align)?,
        };
        // A buffered handle asks once whether its file system lets it read from memory, so it
        // says so before its first read (`crate::resident`); a direct one never reads there.
        let resident = match caching {
            Caching::Direct => crate::resident::Resident::new(),
            Caching::Buffered => crate::resident::Resident::probed(&file)
                .map_err(wrap("ask whether reads from memory are taken"))?,
        };
        let fills = caching == Caching::Direct && sys::journals_allocation(&file);
        Ok(Self {
            file,
            path: path.to_path_buf(),
            caching,
            align,
            layout,
            node,
            resident,
            epoch: crate::resident::Epoch::new(),
            fills,
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
            layout: self.layout,
            node: self.node,
            // A duplicate maps its own window when it first reads from memory, and shares the
            // file system's answer to whether it can.
            resident: self.resident.duplicate(),
            epoch: crate::resident::Epoch::new(),
            // The same file, on the same file system.
            fills: self.fills,
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

    /// The block a writer lays the file out in: the transfer alignment when direct, which a
    /// direct file's layout always was; when buffered, the device's write unit
    /// ([`preferred_block`]), since a buffered file's alignment of one byte is no block a log can
    /// lay out a header or a persist slot in. Taken once, at open.
    pub fn layout_block(&self) -> Alignment {
        self.layout
    }

    /// Whether new space is written with zeros before use (`BlockFile::fills_new_space`): a direct
    /// file on a file system that allocates in place and commits its allocation in its journal at
    /// a flush (ext2, ext3, ext4, XFS: [`journals_allocation`]), where it was measured to remove
    /// the journal's flush from every write after a slot's first (hyper-raft docs/benchmarks.md,
    /// "a slot written whole before its frames"). Nowhere else: on btrfs, which writes out of
    /// place, the zeros were a third of an event-bus log's bytes and its commits waited as long
    /// without them ("zeros on btrfs, measured and dropped"); on macOS's APFS they bought nothing
    /// either; a device node has no file system to commit for it.
    pub fn fills_new_space(&self) -> bool {
        self.fills
    }

    /// Writes all of `buf` at `offset` and makes that write durable before it returns. On Linux a
    /// direct file's write goes with `RWF_DSYNC` (pwritev2(2)): the block layer issues it as a FUA
    /// write where the device advertises FUA (`/sys/block/<dev>/queue/fua`) and the write is an
    /// overwrite the file system need not log, and otherwise writes and flushes, so the device's
    /// whole cache is not flushed for one write. Elsewhere, and where the kernel does not take the
    /// flag, a write and [`DeviceFile::sync_data`] (macOS has no durable write of its own:
    /// `F_BARRIERFSYNC` orders and does not persist).
    pub fn write_durable_at(
        &self,
        buf: &[u8],
        offset: u64,
    ) -> Result<crate::block::Durable, DiskError> {
        #[cfg(target_os = "linux")]
        if self.caching == Caching::Direct {
            self.check_alignment(offset, buf.len())?;
            self.check_address(buf.as_ptr().addr(), offset, buf.len())?;
            match sys::write_durable_at(&self.file, buf, offset) {
                Ok(()) => return Ok(crate::block::Durable::Written),
                // A kernel before 4.7 takes no flag: nothing was written.
                Err(e) if e.raw_os_error() == Some(rustix::io::Errno::OPNOTSUPP.raw_os_error()) => {
                }
                Err(e) => return Err(self.io_error("write durable", e)),
            }
        }
        self.write_all_at(buf, offset)?;
        self.sync_data()?;
        Ok(crate::block::Durable::Flushed)
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

    /// Writes `copies` copies of `fill` back to back from `offset`. On Linux and macOS each call
    /// hands the kernel as many copies as it takes at once (pwritev(2), as many vectors as
    /// `IOV_MAX`, every one the same buffer), so a direct file's device has them all in flight
    /// rather than one at a time: a 256 MiB file written whole with 128 KiB copies beside another
    /// process's `F_FULLFSYNC` writes took 61-66 ms so, and 7.5-9.9 s a copy at a time (mantle
    /// `docs/research/48-first-writes.md` §6). On Windows a file past the cache takes gather writes
    /// of the same kind (`crate::gather`); a buffered one there, a write a copy.
    pub fn write_copies_at(&self, fill: &[u8], offset: u64, copies: u64) -> Result<(), DiskError> {
        self.check_alignment(offset, fill.len())?;
        self.check_address(fill.as_ptr().addr(), offset, fill.len())?;
        let piece = u64::try_from(fill.len()).map_err(|_| self.overflow(offset))?;
        piece
            .checked_mul(copies)
            .and_then(|total| offset.checked_add(total))
            .ok_or_else(|| self.overflow(offset))?;
        #[cfg(unix)]
        {
            sys::write_copies_at(&self.file, fill, offset, copies)
                .map_err(|e| self.io_error("write copies", e))
        }
        #[cfg(windows)]
        if self.caching == Caching::Direct && !self.node {
            return crate::gather::write_copies(&self.path, fill, offset, copies)
                .map_err(|e| self.io_error("write copies", e));
        }
        #[cfg(not(unix))]
        {
            let mut at = offset;
            for _ in 0..copies {
                self.write_all_at(fill, at)?;
                at = at.checked_add(piece).ok_or_else(|| self.overflow(offset))?;
            }
            Ok(())
        }
    }

    /// Fills `buf` from `offset` only if the OS holds every byte of it in memory now, without
    /// waiting for the device (`crate::resident`): true when it read, false when the read would
    /// wait, the file ends first, or the platform cannot tell, leaving the read to a thread that
    /// may wait ([`crate::issuer`]). A direct file has no OS cache to read from, so it never reads
    /// here. On Linux a page not in memory has its read started before the call answers, and the
    /// call reads it if that read finished first: true says the bytes were read without waiting,
    /// not that they were in memory before.
    pub fn read_resident_at(&mut self, buf: &mut [u8], offset: u64) -> Result<bool, DiskError> {
        self.read_resident(buf, offset, None)
    }

    /// [`Self::read_resident_at`], its answers kept in `epoch` rather than this handle's own: one
    /// epoch for the files a caller reads as one (`crate::segmented`).
    pub(crate) fn read_resident_with(
        &mut self,
        buf: &mut [u8],
        offset: u64,
        epoch: &mut crate::resident::Epoch,
    ) -> Result<bool, DiskError> {
        self.read_resident(buf, offset, Some(epoch))
    }

    fn read_resident(
        &mut self,
        buf: &mut [u8],
        offset: u64,
        shared: Option<&mut crate::resident::Epoch>,
    ) -> Result<bool, DiskError> {
        if self.caching == Caching::Direct {
            return Ok(false);
        }
        self.check_alignment(offset, buf.len())?;
        let epoch = match shared {
            Some(epoch) => epoch,
            None => &mut self.epoch,
        };
        match self.resident.read(&self.file, buf, offset, epoch) {
            Ok(read) => Ok(read),
            Err(e) => Err(self.io_error("read from memory", e)),
        }
    }

    /// What [`Self::read_resident_at`] did: probes, reads on kept answers, epochs.
    pub fn residency(&self) -> crate::Residency {
        self.resident.residency().plus(self.epoch.residency())
    }

    /// Shadow mode, for measurement: a read made on a kept answer probes anyway and counts the
    /// answer wrong when the probe disagrees.
    pub fn set_residency_shadow(&mut self, on: bool) {
        self.resident.set_shadow(on);
    }

    /// Whether [`Self::read_resident_at`] can read anything: a buffered file, where the platform
    /// can tell what the OS holds (`crate::resident`) and the file system has not refused to
    /// (overlayfs refuses Linux's `RWF_NOWAIT`).
    pub fn reads_resident(&self) -> bool {
        self.caching == Caching::Buffered
            && cfg!(any(target_os = "linux", target_vendor = "apple"))
            && self.resident.reads()
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

    /// Whether the file system writes the file in place where it would copy it on write: `Some`
    /// on btrfs, whether the file is marked (`FS_NOCOW_FL`, asked at [`DeviceFile::open`] of an
    /// empty file), `None` on a file system that writes every file in place, or that cannot say.
    pub fn written_in_place(&self) -> Option<bool> {
        if self.node {
            return None;
        }
        sys::written_in_place_marked(&self.file)
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

/// A buffered file's layout block: the device's write unit, and no less than `align`. A write unit
/// that is no power of two within the alignment bound is no block to lay a file out in, and is
/// refused rather than rounded: rounded, a block would no longer be a write the device takes whole.
fn layout_of(file: &File, path: &Path, align: Alignment) -> Result<Alignment, DiskError> {
    let unit = preferred_block(file, path)?;
    let unit = Alignment::new(unit).map_err(|_| DiskError::Unsupported {
        path: path.to_path_buf(),
        reason: "a file whose device's write unit is no power of two within the alignment bound",
    })?;
    Ok(if unit.get() >= align.get() {
        unit
    } else {
        align
    })
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

/// Renames `from` to `to` and flushes the directory `to` is in, and `from`'s where it differs, so
/// the new entry survives a crash (Pillai et al., OSDI 2014). The file itself must be flushed
/// first: a rename makes durable only the name.
#[allow(
    clippy::disallowed_methods,
    reason = "the device layer renames the files it keeps into place"
)]
pub fn rename_durable(from: &Path, to: &Path) -> Result<(), DiskError> {
    std::fs::rename(from, to).map_err(|source| DiskError::Io {
        op: "rename",
        path: to.to_path_buf(),
        source,
    })?;
    let parent = |path: &Path| match path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir.to_path_buf(),
        _ => std::path::PathBuf::from("."),
    };
    let (to_dir, from_dir) = (parent(to), parent(from));
    sync_dir(&to_dir)?;
    if from_dir != to_dir {
        sync_dir(&from_dir)?;
    }
    Ok(())
}

fn options(create: bool) -> OpenOptions {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(create);
    options
}

/// ext2's, ext3's and ext4's statfs(2) `f_type`, one for the three
/// (include/uapi/linux/magic.h, `EXT2_SUPER_MAGIC`, `EXT3_SUPER_MAGIC`, `EXT4_SUPER_MAGIC`).
#[cfg(any(target_os = "linux", target_os = "android", test))]
const EXT_SUPER_MAGIC: i128 = 0xEF53;
/// XFS's statfs(2) `f_type` (include/uapi/linux/magic.h, `XFS_SUPER_MAGIC`, "XFSB").
#[cfg(any(target_os = "linux", target_os = "android", test))]
const XFS_SUPER_MAGIC: i128 = 0x5846_5342;

/// Whether the file system of statfs(2) type `f_type` allocates a file's blocks in place and
/// commits that allocation in its journal at a flush, so that a write past the file's size or into
/// an unwritten extent makes the flush commit the journal (fallocate(2); fs/iomap/direct-io.c) and
/// an overwrite of written blocks does not: ext2, ext3 and ext4 and XFS. Not btrfs, whose log
/// files are written in place and whose commits were measured to wait as long without the zeros
/// (hyper-raft docs/benchmarks.md, "zeros on btrfs, measured and dropped"); not a file system that
/// writes every block out of place by design (f2fs, bcachefs), for which a block written ahead is
/// written again elsewhere; not one that keeps nothing (tmpfs), which commits nothing.
#[cfg(any(target_os = "linux", target_os = "android", test))]
fn journals_allocation(f_type: i128) -> bool {
    f_type == EXT_SUPER_MAGIC || f_type == XFS_SUPER_MAGIC
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

    /// `copies` copies of `fill` from `offset` (the caller checked the span fits a file offset),
    /// by pwritev(2) with a vector a copy, all the one buffer: rustix passes at most `IOV_MAX` of
    /// them a call. A call that ends inside a copy has the rest of that copy written alone.
    pub(super) fn write_copies_at(
        file: &File,
        fill: &[u8],
        offset: u64,
        copies: u64,
    ) -> io::Result<()> {
        let invalid = || io::Error::from(io::ErrorKind::InvalidInput);
        let piece = u64::try_from(fill.len()).map_err(|_| invalid())?;
        let count = usize::try_from(copies).map_err(|_| invalid())?;
        if fill.is_empty() {
            return Ok(());
        }
        let mut vectors: Vec<io::IoSlice<'_>> = Vec::new();
        vectors
            .try_reserve_exact(count)
            .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
        vectors.extend((0..count).map(|_| io::IoSlice::new(fill)));
        let at = |copy: usize| {
            u64::try_from(copy)
                .ok()
                .and_then(|c| c.checked_mul(piece))
                .and_then(|b| offset.checked_add(b))
                .ok_or_else(invalid)
        };
        let mut next = 0usize;
        while let Some(rest) = vectors.get(next..).filter(|r| !r.is_empty()) {
            let wrote = match rustix::io::pwritev(file, rest, at(next)?) {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(n) => n,
                Err(e) if e == rustix::io::Errno::INTR => continue,
                Err(e) => return Err(e.into()),
            };
            let whole = wrote.checked_div(fill.len()).ok_or_else(invalid)?;
            let part = wrote.checked_rem(fill.len()).ok_or_else(invalid)?;
            next = next.checked_add(whole).ok_or_else(invalid)?;
            if part > 0 {
                let tail = fill.get(part..).ok_or_else(invalid)?;
                let start = at(next)?
                    .checked_add(u64::try_from(part).map_err(|_| invalid())?)
                    .ok_or_else(invalid)?;
                file.write_all_at(tail, start)?;
                next = next.checked_add(1).ok_or_else(invalid)?;
            }
        }
        Ok(())
    }

    /// All of `buf` at `offset` with `RWF_DSYNC`: each part durable before the call returns.
    #[cfg(target_os = "linux")]
    pub(super) fn write_durable_at(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
        use rustix::io::{ReadWriteFlags, pwritev2};
        let mut done = 0usize;
        while let Some(rest) = buf.get(done..).filter(|r| !r.is_empty()) {
            let at = u64::try_from(done)
                .ok()
                .and_then(|done| offset.checked_add(done))
                .ok_or(io::ErrorKind::InvalidInput)?;
            let wrote = pwritev2(file, &[io::IoSlice::new(rest)], at, ReadWriteFlags::DSYNC)
                .map_err(io::Error::from)?;
            if wrote == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            done = done.saturating_add(wrote);
        }
        Ok(())
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

    /// btrfs's statfs(2) `f_type` (include/uapi/linux/magic.h, `BTRFS_SUPER_MAGIC`).
    #[cfg(any(target_os = "linux", target_os = "android"))]
    const BTRFS_SUPER_MAGIC: i128 = 0x9123_683E;

    /// Whether the file's file system journals its allocation ([`super::journals_allocation`]),
    /// by its statfs(2) `f_type`; a file system that does not answer is taken not to.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub(super) fn journals_allocation(file: &File) -> bool {
        rustix::fs::fstatfs(file).is_ok_and(|s| super::journals_allocation(i128::from(s.f_type)))
    }

    /// Elsewhere no file system focal ships on was measured to pay for the zeros (APFS).
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    pub(super) fn journals_allocation(_file: &File) -> bool {
        false
    }

    /// Asks btrfs to write a still-empty file in place (`FS_NOCOW_FL`, chattr(1)'s `C`), as a
    /// log's file is written: btrfs copies a file on write by default, so every overwrite of the
    /// log's written blocks took new extents from its free-space bitmaps and summed its own
    /// checksums of the data (and, mounted with `compress`, compressed it), where the log sums and
    /// seals its own. An event-bus node on btrfs spent 6 to 25% of its CPU in
    /// `_find_next_zero_bit` and `steal_from_bitmap` on its log's I/O thread (focal on fedora
    /// under perf, 2026-10-19). btrfs takes the flag only on an empty file and leaves a file with
    /// a byte in it as it is, without an error (`btrfs_fileattr_set` sets `NODATACOW` only at an
    /// `i_size` of zero), so it is asked as the file is opened empty; systemd's journal asks the
    /// same of its files. A file system that is not btrfs is left as it is, and a btrfs that
    /// refuses keeps the file as before: written by copying, and as durable.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub(super) fn written_in_place(file: &File) {
        if !rustix::fs::fstatfs(file).is_ok_and(|s| i128::from(s.f_type) == BTRFS_SUPER_MAGIC) {
            return;
        }
        if let Ok(flags) = rustix::fs::ioctl_getflags(file) {
            let _ = rustix::fs::ioctl_setflags(file, flags | rustix::fs::IFlags::NOCOW);
        }
    }

    /// Elsewhere every file system focal ships on writes a file in place.
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    pub(super) fn written_in_place(_file: &File) {}

    /// Whether the file is marked to be written in place: `None` off btrfs.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub(super) fn written_in_place_marked(file: &File) -> Option<bool> {
        let on_btrfs =
            rustix::fs::fstatfs(file).is_ok_and(|s| i128::from(s.f_type) == BTRFS_SUPER_MAGIC);
        if !on_btrfs {
            return None;
        }
        rustix::fs::ioctl_getflags(file)
            .ok()
            .map(|flags| flags.contains(rustix::fs::IFlags::NOCOW))
    }

    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    pub(super) fn written_in_place_marked(_file: &File) -> Option<bool> {
        None
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

    /// NTFS writes a file in place: nothing to ask.
    pub(super) fn written_in_place(_file: &File) {}

    /// NTFS was not measured to pay for the zeros: none are written.
    pub(super) fn journals_allocation(_file: &File) -> bool {
        false
    }

    pub(super) fn written_in_place_marked(_file: &File) -> Option<bool> {
        None
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

    /// Do: open an empty file made beforehand without `create`, as a log's owner opens the file
    /// it made (focal's bus makes it with `File::create`), then write past its end as the log
    /// grows. Expect: on btrfs it is marked to be written in place (`FS_NOCOW_FL`), and stays
    /// so; a file opened with a byte in it is left as it was, copied on write, since btrfs takes
    /// the mark only on an empty file. Elsewhere nothing is asked and nothing is said. Run with
    /// `TMPDIR` on a btrfs to see the mark.
    #[test]
    #[expect(
        clippy::disallowed_methods,
        reason = "the test makes its files outside the device layer, as the log's owner may"
    )]
    fn a_file_opened_empty_on_btrfs_is_written_in_place_and_one_with_bytes_is_left_as_it_was() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        std::fs::File::create(&path).unwrap();
        let file = DeviceFile::open(&path, false, CachingRequest::PreferDirect, align()).unwrap();
        let mut block = AlignedBuf::zeroed(4096, align()).unwrap();
        block.extend_from_slice(&[7; 4096]).unwrap();
        file.write_all_at(block.as_slice(), 1 << 20).unwrap();
        file.sync_data().unwrap();
        let other = dir.path().join("written");
        std::fs::write(&other, [1u8]).unwrap();
        let written = DeviceFile::open(&other, false, CachingRequest::Buffered, align()).unwrap();
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if file.written_in_place().is_some() {
            assert_eq!(
                file.written_in_place(),
                Some(true),
                "a file opened empty on btrfs was left to be copied on write"
            );
            assert_eq!(written.written_in_place(), Some(false));
            return;
        }
        assert_eq!(file.written_in_place(), None);
        assert_eq!(written.written_in_place(), None);
    }

    /// New space is written with zeros only where the file system allocates in place and journals
    /// it: the ext family and XFS, never btrfs, f2fs, bcachefs or tmpfs. A direct file answers by
    /// its own file system's type, a buffered one never fills (`fills_new_space`).
    #[test]
    fn zeros_are_written_ahead_only_where_the_file_system_journals_its_allocation() {
        for (f_type, fills) in [
            (0xEF53, true),
            (0x5846_5342, true),
            (0x9123_683E, false),
            (0xF2F5_2010, false),
            (0xCA45_1A4E, false),
            (0x0102_1994, false),
        ] {
            assert_eq!(journals_allocation(f_type), fills, "{f_type:#x}");
        }
        let dir = tempfile::tempdir().unwrap();
        let direct = DeviceFile::open(
            &dir.path().join("direct"),
            true,
            CachingRequest::PreferDirect,
            align(),
        )
        .unwrap();
        let buffered = DeviceFile::open(
            &dir.path().join("buffered"),
            true,
            CachingRequest::Buffered,
            align(),
        )
        .unwrap();
        assert!(!buffered.fills_new_space());
        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            let f_type = i128::from(rustix::fs::statfs(dir.path()).unwrap().f_type);
            assert_eq!(
                direct.fills_new_space(),
                direct.caching == Caching::Direct && journals_allocation(f_type),
                "{f_type:#x}"
            );
        }
        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        assert!(!direct.fills_new_space());
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

    /// A buffered file is laid out in the device's write unit, its alignment of one byte being no
    /// block; a direct one in its transfer alignment, as a direct file always was, so the layout of
    /// a log already on it is unchanged.
    #[test]
    fn a_buffered_file_is_laid_out_in_the_write_unit_and_a_direct_one_in_its_alignment() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("buffered");
        let buffered = DeviceFile::open(&path, true, CachingRequest::Buffered, align()).unwrap();
        assert_eq!(buffered.alignment(), Alignment::BYTE);
        let unit = preferred_block(buffered.std_file(), &path).unwrap();
        assert_eq!(buffered.layout_block().get(), unit);
        assert_eq!(buffered.try_clone().unwrap().layout_block().get(), unit);
        let direct = DeviceFile::open(
            &dir.path().join("direct"),
            true,
            CachingRequest::PreferDirect,
            align(),
        )
        .unwrap();
        if direct.caching() == Caching::Direct {
            assert_eq!(direct.layout_block(), direct.alignment());
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

    /// Do: write pages through the cache; read one from memory twice with the kernel's epoch
    /// held; again in shadow mode; then once after the epoch moves. Expect: the first read probes,
    /// the second is made on its kept answer, the shadow read probes anyway and finds the answer
    /// right, and after the move the read probes again; every read returns the page's bytes.
    #[cfg(target_vendor = "apple")]
    #[test]
    fn a_page_found_in_memory_is_read_again_without_asking_until_the_epoch_moves() {
        let dir = tempfile::tempdir().unwrap();
        let mut file = DeviceFile::open(
            &dir.path().join("answers"),
            true,
            CachingRequest::Buffered,
            align(),
        )
        .unwrap();
        let page = 4096usize;
        let bytes: Vec<u8> = (0..4 * page).map(|i| (i % 251) as u8).collect();
        file.write_all_at(&bytes, 0).unwrap();
        file.epoch.hold();
        let mut buf = vec![0u8; page];
        let mut read = |file: &mut DeviceFile| {
            buf.fill(0);
            assert!(file.read_resident_at(&mut buf, page as u64).unwrap());
            assert_eq!(buf, &bytes[page..2 * page]);
            let r = file.residency();
            (r.probes, r.skips, r.wrong)
        };
        assert_eq!(read(&mut file), (1, 0, 0));
        assert_eq!(read(&mut file), (1, 1, 0));
        file.set_residency_shadow(true);
        assert_eq!(read(&mut file), (2, 2, 0));
        file.set_residency_shadow(false);
        file.epoch.move_on();
        assert_eq!(read(&mut file), (3, 2, 0));
        assert_eq!(read(&mut file), (3, 3, 0));
    }

    /// Do: refresh a reader against the kernel's reclamation counters, then again with its epoch
    /// held one off them. Expect: the second refresh counts a change and starts a new generation,
    /// and each sets when the epoch is next read.
    #[cfg(target_vendor = "apple")]
    #[test]
    fn a_refresh_that_finds_the_epoch_moved_forgets_every_answer() {
        let mut epoch = crate::resident::Epoch::new();
        let (before, after) = epoch.refresh_from_off();
        assert_eq!(after, before + 1);
    }

    /// Do: write pages through the cache, then read them only from memory: one the write left
    /// there, one past the file's end, one written far past the window the first read mapped, and
    /// one through a duplicate. Expect: where the platform can tell, each page in memory is read
    /// here with the file's bytes and the one past the end is not; where it cannot (or the file
    /// system refuses `RWF_NOWAIT`), none is, and the handle says so.
    #[test]
    fn a_buffered_file_reads_what_memory_holds() {
        let dir = tempfile::tempdir().unwrap();
        let mut file = DeviceFile::open(
            &dir.path().join("resident"),
            true,
            CachingRequest::Buffered,
            align(),
        )
        .unwrap();
        let page = 4096usize;
        let bytes: Vec<u8> = (0..4 * page).map(|i| (i % 251) as u8).collect();
        file.write_all_at(&bytes, 0).unwrap();
        let mut buf = vec![0u8; page];
        let read = file.read_resident_at(&mut buf, page as u64).unwrap();
        // A regular file on macOS is always mapped: its probes never refuse.
        assert!(!(cfg!(target_vendor = "apple") && file.resident.refused()));
        assert_eq!(read, !file.resident.refused());
        if read {
            assert_eq!(buf, &bytes[page..2 * page]);
        }
        assert!(!file.read_resident_at(&mut buf, 8 * page as u64).unwrap());
        // Far past any window mapped so far: the mapping grows to reach it.
        let far = 1u64 << 22;
        file.write_all_at(&bytes[..page], far).unwrap();
        buf.fill(0);
        assert_eq!(
            file.read_resident_at(&mut buf, far).unwrap(),
            !file.resident.refused()
        );
        if !file.resident.refused() {
            assert_eq!(buf, &bytes[..page]);
        }
        let mut twin = file.try_clone().unwrap();
        buf.fill(0);
        assert_eq!(
            twin.read_resident_at(&mut buf, 0).unwrap(),
            !twin.resident.refused()
        );
        if !twin.resident.refused() {
            assert_eq!(buf, &bytes[..page]);
        }
    }

    /// Do: write more copies of a page than one vectored call takes (1024 is `IOV_MAX` on Linux
    /// and macOS) from past a hole, through a buffered handle and one past the cache. Expect:
    /// every copy lands back to back, and the file ends with the last.
    #[test]
    fn copies_of_a_page_land_back_to_back_past_a_calls_vectors() {
        for caching in [CachingRequest::Buffered, CachingRequest::PreferDirect] {
            let dir = tempfile::tempdir().unwrap();
            let file =
                DeviceFile::open(&dir.path().join("copies"), true, caching, align()).unwrap();
            let mut fill = AlignedBuf::zeroed(4096, align()).unwrap();
            fill.extend_from_slice(&[0xa5; 4096]).unwrap();
            let copies = 1500u64;
            file.write_copies_at(fill.as_slice(), 4096, copies).unwrap();
            assert_eq!(file.len().unwrap(), 4096 * (copies + 1));
            let mut back = AlignedBuf::zeroed(4096, align()).unwrap();
            back.extend_from_slice(&[0; 4096]).unwrap();
            for k in 1..=copies {
                back.as_mut_slice().fill(0);
                file.read_exact_at(back.as_mut_slice(), 4096 * k).unwrap();
                assert_eq!(back.as_slice(), fill.as_slice(), "copy {k}");
            }
            // A direct handle refuses copies at an offset off its alignment.
            assert!(
                file.write_copies_at(fill.as_slice(), 1, 1).is_err()
                    || file.caching() == Caching::Buffered
            );
        }
    }

    /// Do: read a direct file only from memory. Expect: nothing read, without a transfer: a
    /// direct file has no OS cache to read from.
    #[test]
    fn a_direct_file_reads_nothing_from_memory() {
        let dir = tempfile::tempdir().unwrap();
        let mut file = DeviceFile::open(
            &dir.path().join("direct"),
            true,
            CachingRequest::PreferDirect,
            align(),
        )
        .unwrap();
        if file.caching() == Caching::Direct {
            let mut buf = vec![0u8; 4096];
            assert!(!file.read_resident_at(&mut buf, 0).unwrap());
        }
    }
}
