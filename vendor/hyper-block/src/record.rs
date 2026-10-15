//! A small record kept whole on a file system, as a node keeps what it must read back after a
//! crash (its run, `hyper_liveness::Settings::run`): written to a temporary name, flushed with the
//! platform's full flush (`File::sync_data`: fdatasync(2) on Linux, `F_FULLFSYNC` on macOS,
//! `FlushFileBuffers` on Windows, as `file` verifies of std), renamed over the record, and its
//! directory flushed after ([`sync_dir`]; Pillai et al., OSDI 2014: a renamed file's entry is
//! durable only once its directory is). A crash leaves the old record or the new one whole. The
//! record carries its CRC-32C (RFC 3720 §12.1), checked when it is read: a record that fails it, or
//! that is longer than its reader's bound, is refused as corrupt and never taken.

use std::fs::File;
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};

use crate::DiskError;
use crate::file::sync_dir;

/// The checksum after the record's bytes: a CRC-32C, little-endian.
const CHECKSUM_BYTES: usize = 4;

fn io(op: &'static str, path: &Path) -> impl FnOnce(std::io::Error) -> DiskError {
    let path = path.to_path_buf();
    move |source| DiskError::Io { op, path, source }
}

/// The directory a record at `path` is renamed in: its parent, or the working directory for a
/// bare name.
fn directory(path: &Path) -> PathBuf {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

/// The record at `path`, `None` where none was ever written. One whose checksum fails, or longer
/// than `limit` bytes (every record has a largest size its reader knows), is refused as corrupt.
pub fn read(path: &Path, limit: usize) -> Result<Option<Vec<u8>>, DiskError> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(io("open", path)(e)),
    };
    let most = limit.saturating_add(CHECKSUM_BYTES);
    let mut bytes = Vec::new();
    // One byte past the bound tells a record too long from one at it.
    file.take(u64::try_from(most).unwrap_or(u64::MAX).saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(io("read", path))?;
    let corrupt = || DiskError::Corrupt {
        path: path.to_path_buf(),
        what: "a record longer than its bound, or whose CRC-32C fails",
    };
    if bytes.len() > most {
        return Err(corrupt());
    }
    let body = bytes
        .len()
        .checked_sub(CHECKSUM_BYTES)
        .ok_or_else(corrupt)?;
    let (record, checksum) = bytes.split_at(body);
    let stated = <[u8; CHECKSUM_BYTES]>::try_from(checksum).map_err(|_| corrupt())?;
    if u32::from_le_bytes(stated) != crc32c::crc32c(record) {
        return Err(corrupt());
    }
    bytes.truncate(body);
    Ok(Some(bytes))
}

/// `record` kept at `path`, whole or not at all, and durable when this returns.
#[allow(
    clippy::disallowed_methods,
    reason = "the device layer writes the records it keeps: a temporary file renamed over the record"
)]
pub fn write(path: &Path, record: &[u8]) -> Result<(), DiskError> {
    let mut name = path.as_os_str().to_owned();
    name.push(".new");
    let temporary = PathBuf::from(name);
    {
        let mut file = File::create(&temporary).map_err(io("create", &temporary))?;
        file.write_all(record).map_err(io("write", &temporary))?;
        file.write_all(&crc32c::crc32c(record).to_le_bytes())
            .map_err(io("write", &temporary))?;
        file.sync_data().map_err(io("sync_data", &temporary))?;
    }
    std::fs::rename(&temporary, path).map_err(io("rename", path))?;
    sync_dir(&directory(path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_record_reads_back_as_written_and_a_missing_one_is_none() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("run");
        assert!(read(&path, 8).unwrap().is_none());
        write(&path, &7u64.to_le_bytes()).unwrap();
        assert_eq!(read(&path, 8).unwrap(), Some(7u64.to_le_bytes().to_vec()));
        write(&path, &8u64.to_le_bytes()).unwrap();
        assert_eq!(read(&path, 8).unwrap(), Some(8u64.to_le_bytes().to_vec()));
    }

    #[test]
    #[allow(
        clippy::disallowed_methods,
        reason = "a test damages the record it wrote"
    )]
    fn a_damaged_or_overlong_record_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("run");
        write(&path, &7u64.to_le_bytes()).unwrap();
        assert!(read(&path, 4).is_err(), "longer than its bound");
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[0] ^= 1;
        std::fs::write(&path, &bytes).unwrap();
        assert!(matches!(read(&path, 8), Err(DiskError::Corrupt { .. })));
        std::fs::write(&path, [1, 2]).unwrap();
        assert!(matches!(read(&path, 8), Err(DiskError::Corrupt { .. })));
    }
}
