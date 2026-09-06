//! Explicit terminal-upload metadata. Its magic is incompatible with the old
//! schema-one UploadMeta decoder; historical active metadata is not reinterpreted.
use super::{ContentError, UploadId, read_bounded, sync_directory};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
};

const MAGIC: &[u8; 8] = b"FCLUPT02";
pub(super) const RECORD_BYTES: usize = 56;

pub(super) fn decode(bytes: &[u8]) -> Result<Option<UploadId>, ContentError> {
    if !bytes.starts_with(MAGIC) {
        return Ok(None);
    }
    if bytes.len() != RECORD_BYTES {
        return Err(ContentError::Corrupt);
    }
    let body = bytes.get(..24).ok_or(ContentError::Corrupt)?;
    if bytes.get(24..) != Some(blake3::hash(body).as_bytes().as_slice()) {
        return Err(ContentError::Corrupt);
    }
    let id = bytes
        .get(8..24)
        .ok_or(ContentError::Corrupt)?
        .try_into()
        .map_err(|_| ContentError::Corrupt)?;
    Ok(Some(UploadId(id)))
}

fn regular(path: &Path) -> Result<bool, ContentError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    if !metadata.is_file() {
        return Err(ContentError::Corrupt);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(ContentError::Corrupt);
        }
    }
    Ok(true)
}
pub(super) fn contains(path: &Path, id: UploadId) -> Result<bool, ContentError> {
    if !regular(path)? {
        return Ok(false);
    }
    let bytes = read_bounded(path, RECORD_BYTES)?;
    if decode(&bytes)? != Some(id) {
        return Err(ContentError::Corrupt);
    }
    Ok(true)
}

pub(super) fn install(path: &Path, id: UploadId) -> Result<(), ContentError> {
    let mut bytes = [0; RECORD_BYTES];
    bytes
        .get_mut(..8)
        .ok_or(ContentError::Corrupt)?
        .copy_from_slice(MAGIC);
    bytes
        .get_mut(8..24)
        .ok_or(ContentError::Corrupt)?
        .copy_from_slice(&id.0);
    let checksum = blake3::hash(bytes.get(..24).ok_or(ContentError::Corrupt)?);
    bytes
        .get_mut(24..)
        .ok_or(ContentError::Corrupt)?
        .copy_from_slice(checksum.as_bytes());
    let temp = path.with_extension("terminal");
    if regular(&temp)? {
        fs::remove_file(&temp)?;
    }
    // The caller holds ContentStore's sole writer lock. Never follow a pending
    // symlink or truncate an unexpected file while recovering an interrupted cut.
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fault(Cut::FileSynced)?;
    fs::rename(&temp, path)?;
    fault(Cut::Renamed)?;
    sync_directory(path.parent().ok_or(ContentError::Invalid)?)?;
    Ok(())
}
pub(super) fn remove_part(path: &Path) -> Result<(), ContentError> {
    if regular(path)? {
        fs::remove_file(path)?;
        sync_directory(path.parent().ok_or(ContentError::Invalid)?)?;
    }
    Ok(())
}

pub(super) fn discard_pending(path: &Path) -> Result<(), ContentError> {
    let name = path
        .file_stem()
        .and_then(|value| value.to_str())
        .ok_or(ContentError::Corrupt)?;
    if name.len() != 32
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ContentError::Corrupt);
    }
    // This name was never atomically published as metadata. Even a partial
    // write is unacknowledged and can be removed without forgetting a fence.
    if regular(path)? {
        if fs::metadata(path)?.len() > RECORD_BYTES as u64 {
            return Err(ContentError::Corrupt);
        }
        fs::remove_file(path)?;
        sync_directory(path.parent().ok_or(ContentError::Invalid)?)?;
    }
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Cut {
    FileSynced,
    Renamed,
}
#[cfg(not(test))]
fn fault(_: Cut) -> Result<(), ContentError> {
    Ok(())
}
#[cfg(test)]
std::thread_local! { static CUT: std::cell::Cell<Option<Cut>> = const { std::cell::Cell::new(None) }; }
#[cfg(test)]
fn fault(cut: Cut) -> Result<(), ContentError> {
    if CUT.with(|pending| {
        if pending.get() == Some(cut) {
            pending.set(None);
            true
        } else {
            false
        }
    }) {
        return Err(std::io::Error::other("injected terminal-upload publication cut").into());
    }
    Ok(())
}

#[cfg(test)]
#[path = "upload_terminal_tests.rs"]
mod tests;
