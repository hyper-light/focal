//! The positional file operations the log and the chunk store are written against, so a test can
//! put a simulated device with power-loss semantics (`crate::sim`) where the real file goes.
//!
//! A file has one owner at a time: the thread that issues its I/O. The trait asks `Send` and not
//! `Sync`, so a file moves to its owner and is never shared; a second thread that needs the same
//! file gets a second handle of its own ([`BlockFile::try_clone`]), as the issuer's workers do.

use crate::DiskError;
use crate::buf::Alignment;
use crate::file::DeviceFile;

/// How a write was made durable by [`BlockFile::write_durable_at`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durable {
    /// The write, then a flush of the file: the device's whole volatile cache.
    Flushed,
    /// One write the device makes durable on its own (`RWF_DSYNC` on a direct file: a FUA write
    /// where the device has FUA, which the kernel otherwise emulates with a flush the file does
    /// not see), the rest of the cache left as it was.
    Written,
}

/// A file read and written at offsets, whose flush makes completed writes durable.
pub trait BlockFile: Send {
    /// The alignment every transfer's offset and length must meet.
    fn alignment(&self) -> Alignment;

    /// The block a writer lays the file out in: what it pads records to and places them at, so
    /// that each write is one the device takes whole. A multiple of [`BlockFile::alignment`], so
    /// what is laid out at it is transferable; by default the alignment itself.
    fn layout_block(&self) -> Alignment {
        self.alignment()
    }

    /// The file's length, or a device node's capacity.
    fn len(&self) -> Result<u64, DiskError>;

    /// Whether the file holds no bytes.
    fn is_empty(&self) -> Result<bool, DiskError> {
        self.len().map(|len| len == 0)
    }

    /// Fills `buf` from `offset`; reaching the end of the file first is an error.
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError>;

    /// Fills `buf` from `offset` only if the OS holds every byte of it in memory now, without
    /// waiting for the device: true when it read; false when the read would wait, leaving it to
    /// a thread that may ([`crate::issuer`]). By default false: a file that cannot tell reads
    /// nothing here.
    fn read_resident_at(&mut self, _buf: &mut [u8], _offset: u64) -> Result<bool, DiskError> {
        Ok(false)
    }

    /// Whether [`BlockFile::read_resident_at`] can read anything: what an issuer asks before it
    /// keeps a submitter a duplicate to read with ([`crate::issuer::Attached::read_resident_at`]).
    /// False by default.
    fn reads_resident(&self) -> bool {
        false
    }

    /// What [`BlockFile::read_resident_at`] did. Nothing counted by default.
    fn residency(&self) -> crate::Residency {
        crate::Residency::default()
    }

    /// Shadow mode for [`BlockFile::read_resident_at`]'s kept answers. Nothing by default.
    fn set_residency_shadow(&mut self, _on: bool) {}

    /// Writes all of `buf` at `offset`, extending the file if it ends past the end.
    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError>;

    /// Makes every completed write durable. A failure leaves durability of those writes
    /// unknown (Rebello et al., ATC 2020): the caller must stop trusting what it wrote.
    fn sync_data(&self) -> Result<(), DiskError>;

    /// Whether a writer should write the space it grows the file by with zeros once, before it
    /// uses it, so that its later writes there are overwrites: on Linux's ext4 and XFS a write
    /// past the file's size, or into an unwritten extent, changes metadata the journal must commit
    /// at the next flush (fallocate(2); fs/iomap/direct-io.c), and an overwrite of written blocks
    /// does not. False by default: elsewhere the gain is not measured, and the zeros cost a write.
    fn fills_new_space(&self) -> bool {
        false
    }

    /// Whether the file grows by segments ([`BlockFile::grow`], `crate::segmented`) rather than
    /// by writes past its end. False by default.
    fn grows_by_segments(&self) -> bool {
        false
    }

    /// Adds a segment of `len` bytes after the file's end, allocated whole and its name durable,
    /// when the file grows by segments: the offset its first byte is at. Its blocks are written by
    /// the caller ([`BlockFile::write_whole`]) before any other transfer uses them, so the
    /// segments in use take no first write (`crate::segmented`). None for a file that grows by
    /// writes past its end.
    fn grow(&self, _len: u64) -> Result<Option<u64>, DiskError> {
        Ok(None)
    }

    /// Writes `copies` copies of `fill` back to back from `offset`, into space no other transfer
    /// uses yet: its blocks written once, so that every later write there is an overwrite. A
    /// flush of the file makes them durable. By default, a write of each copy.
    fn write_whole(&self, offset: u64, copies: u64, fill: &[u8]) -> Result<(), DiskError> {
        let piece = u64::try_from(fill.len()).unwrap_or(u64::MAX);
        let mut at = offset;
        for _ in 0..copies {
            self.write_all_at(fill, at)?;
            at = at.checked_add(piece).ok_or_else(|| DiskError::Io {
                op: "write whole",
                path: std::path::PathBuf::new(),
                source: std::io::Error::from(std::io::ErrorKind::InvalidInput),
            })?;
        }
        Ok(())
    }

    /// The end of the segment holding `offset`, when the file grows by segments and one does.
    /// None otherwise.
    fn segment_end(&self, _offset: u64) -> Result<Option<u64>, DiskError> {
        Ok(None)
    }

    /// Writes all of `buf` at `offset` and makes that write durable before it returns: at least
    /// that write, and of the others not yet flushed none it promises. By default a write and a
    /// flush of the file. A failure leaves the write's durability unknown, as a flush's does.
    fn write_durable_at(&self, buf: &[u8], offset: u64) -> Result<Durable, DiskError> {
        self.write_all_at(buf, offset)?;
        self.sync_data()?;
        Ok(Durable::Flushed)
    }

    /// A second handle to the same file, whose writes and flushes are the file's own: what a
    /// volume hands its device's issuer while it keeps reading through its own handle
    /// (crate::issuer). A file that has no second handle refuses.
    fn try_clone(&self) -> Result<Self, DiskError>
    where
        Self: Sized,
    {
        Err(DiskError::Io {
            op: "duplicate a file handle",
            path: std::path::PathBuf::new(),
            source: std::io::Error::from(std::io::ErrorKind::Unsupported),
        })
    }
}

impl BlockFile for DeviceFile {
    fn alignment(&self) -> Alignment {
        DeviceFile::alignment(self)
    }

    fn layout_block(&self) -> Alignment {
        DeviceFile::layout_block(self)
    }

    fn len(&self) -> Result<u64, DiskError> {
        DeviceFile::len(self)
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
        DeviceFile::read_exact_at(self, buf, offset)
    }

    fn read_resident_at(&mut self, buf: &mut [u8], offset: u64) -> Result<bool, DiskError> {
        DeviceFile::read_resident_at(self, buf, offset)
    }

    fn reads_resident(&self) -> bool {
        DeviceFile::reads_resident(self)
    }

    fn residency(&self) -> crate::Residency {
        DeviceFile::residency(self)
    }

    fn set_residency_shadow(&mut self, on: bool) {
        DeviceFile::set_residency_shadow(self, on);
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
        DeviceFile::write_all_at(self, buf, offset)
    }

    fn sync_data(&self) -> Result<(), DiskError> {
        DeviceFile::sync_data(self)
    }

    fn write_durable_at(&self, buf: &[u8], offset: u64) -> Result<Durable, DiskError> {
        DeviceFile::write_durable_at(self, buf, offset)
    }

    fn fills_new_space(&self) -> bool {
        DeviceFile::fills_new_space(self)
    }

    fn try_clone(&self) -> Result<Self, DiskError> {
        DeviceFile::try_clone(self)
    }
}
