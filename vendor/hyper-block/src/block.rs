//! The positional file operations the log and the chunk store are written against, so a test can
//! put a simulated device with power-loss semantics (`crate::sim`) where the real file goes.
//!
//! A file has one owner at a time: the thread that issues its I/O. The trait asks `Send` and not
//! `Sync`, so a file moves to its owner and is never shared; a second thread that needs the same
//! file gets a second handle of its own ([`BlockFile::try_clone`]), as the issuer's workers do.

use crate::DiskError;
use crate::buf::Alignment;
use crate::file::DeviceFile;

/// A file read and written at offsets, whose flush makes completed writes durable.
pub trait BlockFile: Send {
    /// The alignment every transfer's offset and length must meet.
    fn alignment(&self) -> Alignment;

    /// The file's length, or a device node's capacity.
    fn len(&self) -> Result<u64, DiskError>;

    /// Whether the file holds no bytes.
    fn is_empty(&self) -> Result<bool, DiskError> {
        self.len().map(|len| len == 0)
    }

    /// Fills `buf` from `offset`; reaching the end of the file first is an error.
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError>;

    /// Writes all of `buf` at `offset`, extending the file if it ends past the end.
    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError>;

    /// Makes every completed write durable. A failure leaves durability of those writes
    /// unknown (Rebello et al., ATC 2020): the caller must stop trusting what it wrote.
    fn sync_data(&self) -> Result<(), DiskError>;

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

    fn len(&self) -> Result<u64, DiskError> {
        DeviceFile::len(self)
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
        DeviceFile::read_exact_at(self, buf, offset)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
        DeviceFile::write_all_at(self, buf, offset)
    }

    fn sync_data(&self) -> Result<(), DiskError> {
        DeviceFile::sync_data(self)
    }

    fn try_clone(&self) -> Result<Self, DiskError> {
        DeviceFile::try_clone(self)
    }
}
