//! Whole files sealed under a store's key (29 §5): a file made whole in memory, sealed by STREAM
//! (`hyper_seal::sealed_file`) under a data key of its own wrapped by the store's key, and installed
//! by its writer's atomic replace; read whole, then opened. One data key a file, so no file's nonce
//! repeats under another's key, and a file rewritten is sealed under a new key.
//!
//! The group files (focal-consensus `group_files`), the enrollment's private directories and every
//! journal and small file seal through here.
use std::cell::RefCell;
use std::path::PathBuf;

use hyper_seal::keys::WrappingKey;
use hyper_seal::sealed_file::{SealedFileError, SealedReader, SealedWriter};
use zeroize::Zeroizing;

/// Plaintext bytes of a sealed file's segment: 64 KiB, the segment hyper-seal's sealed-file bench
/// measures (docs/benchmarks.md "Sealed files"); a journal or a small file fits one.
pub const SEGMENT: u32 = 64 * 1024;

/// Why a sealed file was not made or opened.
#[derive(Debug, thiserror::Error)]
pub enum FileError {
    /// The seal refused it: tampered, cut, extended, or under another key.
    #[error("sealed file: {0}")]
    Seal(#[from] SealedFileError),
    /// Its plaintext is larger than the bound its reader gave.
    #[error("a sealed file's plaintext past its bound of {0} bytes")]
    Bound(usize),
}

/// A file held in memory, for a sealed file made whole before it is installed and opened whole once
/// read: byte-aligned, flushed by the install that writes it.
#[derive(Default)]
struct MemFile(RefCell<Vec<u8>>);

impl hyper_block::block::BlockFile for MemFile {
    fn alignment(&self) -> hyper_block::buf::Alignment {
        hyper_block::buf::Alignment::BYTE
    }
    fn len(&self) -> Result<u64, hyper_block::DiskError> {
        u64::try_from(self.0.borrow().len()).map_err(|_| short(0, 0))
    }
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), hyper_block::DiskError> {
        let bytes = self.0.borrow();
        let from = usize::try_from(offset).map_err(|_| short(offset, buf.len()))?;
        let to = from
            .checked_add(buf.len())
            .ok_or(short(offset, buf.len()))?;
        let src = bytes.get(from..to).ok_or(short(offset, buf.len()))?;
        buf.copy_from_slice(src);
        Ok(())
    }
    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), hyper_block::DiskError> {
        let mut bytes = self.0.borrow_mut();
        let from = usize::try_from(offset).map_err(|_| short(offset, buf.len()))?;
        let to = from
            .checked_add(buf.len())
            .ok_or(short(offset, buf.len()))?;
        let grow = to.saturating_sub(bytes.len());
        if grow > 0 {
            bytes
                .try_reserve(grow)
                .map_err(|_| short(offset, buf.len()))?;
            bytes.resize(to, 0);
        }
        bytes
            .get_mut(from..to)
            .ok_or(short(offset, buf.len()))?
            .copy_from_slice(buf);
        Ok(())
    }
    fn sync_data(&self) -> Result<(), hyper_block::DiskError> {
        Ok(())
    }
}

fn short(offset: u64, missing: usize) -> hyper_block::DiskError {
    hyper_block::DiskError::ShortRead {
        path: PathBuf::new(),
        offset,
        missing,
    }
}

/// Bytes of a sealed file's footer: the stream's length (`hyper_seal::sealed_file`).
const FOOTER: u64 = 8;

/// The most bytes a file sealed here takes on its medium for `plain` bytes of plaintext: the
/// STREAM of them (`hyper_seal::stream::sealed_len`) and the footer, the file held byte-aligned so
/// never padded. What a reader reads at most before it opens one.
pub fn sealed_bound(plain: usize) -> Option<usize> {
    let plain = u64::try_from(plain).ok()?;
    let sealed = hyper_seal::stream::sealed_len(plain, SEGMENT).ok()?;
    usize::try_from(sealed.checked_add(FOOTER)?).ok()
}

/// `plain` sealed whole under a new data key wrapped by `key`: the bytes to install.
pub fn seal(key: &WrappingKey, plain: &[u8]) -> Result<Vec<u8>, FileError> {
    let mut writer = SealedWriter::new(MemFile::default(), key, SEGMENT)?;
    writer.write(plain)?;
    Ok(writer.finish()?.0.into_inner())
}

/// The plaintext the sealed file `bytes` holds, opened under `key`, no more than `most` bytes of
/// it; wiped wherever it is let go, as what it holds may be a secret.
pub fn open(
    key: &WrappingKey,
    bytes: Vec<u8>,
    most: usize,
) -> Result<Zeroizing<Vec<u8>>, FileError> {
    let mut reader = SealedReader::open(MemFile(RefCell::new(bytes)), key, SEGMENT)?;
    let len = usize::try_from(reader.len())
        .ok()
        .filter(|len| *len <= most)
        .ok_or(FileError::Bound(most))?;
    let mut plain = Zeroizing::new(Vec::new());
    plain
        .try_reserve_exact(len)
        .map_err(|_| FileError::Bound(most))?;
    plain.resize(len, 0);
    reader.read_at(&mut plain, 0)?;
    Ok(plain)
}
