//! The WAL directory's side of the move to hyper-log (focal 27 §15.8): what the directory's fence states
//! about where the node's groups live, the conversion's commit point, and the old segments moved aside.
//!
//! **The commit point** is a fence of version 3 ([`commit`]): version 2's fields first, then the id of the
//! hyper-log log the groups now live in, summed and bounded as every fence is. A binary of version 2 reads
//! it and refuses to open, `LogError::Identity`, before any replica opens (its `open` compares the version
//! first), so a node never runs the old WAL behind the converted log. This binary refuses it too,
//! `LogError::Converted`: past the commit point the WAL is read only by the conversion's reader.
//!
//! **The old segments** stay until the operator removes them ([`move_segments`], then `focal remove
//! converted-storage`): never removed by the conversion, so an old binary never finds a WAL
//! directory with no fence and makes a fresh, empty WAL — a voter that forgot what it acknowledged.
use super::*;

/// Format: the fence's version once the groups moved to hyper-log.
pub(crate) const FENCE_CONVERTED: u32 = 3;

/// The fence of version 3: version 2's fields, then the hyper-log log's id.
#[derive(Serialize, Deserialize)]
struct ConvertedFence {
    version: u32,
    identity: WalIdentity,
    position: DurablePosition,
    base: DurableBase,
    log: u128,
}

/// Derived: the longest postcard encoding of a [`ConvertedFence`]: a version 2 fence's
/// ([`FENCE_BODY_MAX`]) and a `u128` varint's 19 bytes.
const CONVERTED_BODY_MAX: usize = FENCE_BODY_MAX + 19;
/// Derived: the fence file at its longest: the magic, the body and its CRC-32.
const CONVERTED_FILE_MAX: usize = FENCE_MAGIC.len() + CONVERTED_BODY_MAX + 4;

/// Where a data directory's groups live, as its WAL directory's fence states it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Storage {
    /// focal-log's WAL: a fence of version 1 or 2, or none yet.
    Wal,
    /// hyper-log log `log`, since the conversion's commit point.
    Converted {
        /// The hyper-log log's id.
        log: u128,
    },
}

/// The fence's version and, for version 3, the log it names.
fn read_any(path: &Path) -> Result<(u32, Option<ConvertedFence>), LogError> {
    let bytes = fence_payload(path)?;
    let (version, _) = postcard::take_from_bytes::<u32>(&bytes)
        .map_err(|_| corrupt(path, 0, "invalid fence payload"))?;
    if version != FENCE_CONVERTED {
        return Ok((version, None));
    }
    let fence: ConvertedFence =
        postcard::from_bytes(&bytes).map_err(|_| corrupt(path, 0, "invalid fence payload"))?;
    Ok((version, Some(fence)))
}

/// Where the groups of the data directory whose WAL directory is `directory` live. A directory with no
/// fence holds a WAL that acknowledged nothing yet, or none at all.
pub fn storage(directory: &Path) -> Result<Storage, LogError> {
    let current = FencePaths::of(directory).current;
    if !current.exists() {
        return Ok(Storage::Wal);
    }
    match read_any(&current)? {
        (_, Some(fence)) => Ok(Storage::Converted { log: fence.log }),
        (1 | FENCE_VERSION, None) => Ok(Storage::Wal),
        _ => Err(corrupt(&current, 0, "unknown fence version")),
    }
}

/// The directory's lock, held while the fence or the segments change.
fn lock(directory: &Path) -> Result<focal_platform::FileLock, LogError> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(directory.join("LOCK"))?;
    focal_platform::FileLock::exclusive(file).map_err(|e| {
        if e.kind() == std::io::ErrorKind::WouldBlock {
            LogError::Locked
        } else {
            LogError::Io(e)
        }
    })
}

/// The conversion's commit point (focal 27 §15.8, step 6): replaces the WAL directory's fence of version 2
/// with one of version 3 naming hyper-log log `log`, atomically and durably. The WAL must be closed: the
/// directory's lock is taken here. A fence already of version 3 for the same log is left as it is (the
/// step is repeated after a crash), and one for another log is refused.
pub fn commit(directory: &Path, identity: WalIdentity, log: u128) -> Result<(), LogError> {
    let _lock = lock(directory)?;
    let paths = FencePaths::of(directory);
    let fence = match read_any(&paths.current)? {
        (_, Some(converted)) => {
            return if converted.log == log && converted.identity == identity {
                Ok(())
            } else {
                Err(LogError::Identity)
            };
        }
        (1 | FENCE_VERSION, None) => read_fence(&paths.current)?,
        _ => return Err(corrupt(&paths.current, 0, "unknown fence version")),
    };
    if fence.identity != identity {
        return Err(LogError::Identity);
    }
    let mut file_bytes = [0u8; CONVERTED_FILE_MAX];
    let (magic, rest) = file_bytes.split_at_mut(FENCE_MAGIC.len());
    magic.copy_from_slice(FENCE_MAGIC);
    let body_len = postcard::to_slice(
        &ConvertedFence {
            version: FENCE_CONVERTED,
            identity: fence.identity,
            position: fence.position,
            base: fence.base,
            log,
        },
        rest,
    )?
    .len();
    let crc_at = FENCE_MAGIC.len().saturating_add(body_len);
    let checksum = crc32fast::hash(file_bytes.get(FENCE_MAGIC.len()..crc_at).unwrap_or(&[]));
    let end = crc_at.saturating_add(4);
    file_bytes
        .get_mut(crc_at..end)
        .ok_or(LogError::Capacity)?
        .copy_from_slice(&checksum.to_le_bytes());
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&paths.temp)?;
    file.write_all(file_bytes.get(..end).ok_or(LogError::Capacity)?)?;
    file.sync_all()?;
    focal_platform::fs::atomic_replace(&paths.temp, &paths.current)?;
    sync_dir(directory)
}

/// Whether `name` is one of the WAL's segment files.
fn is_segment(name: &std::ffi::OsStr) -> bool {
    name.to_str()
        .is_some_and(|name| name.starts_with("wal-") && name.ends_with(".seg"))
}

/// Moves the converted WAL's segments from `directory` into `converted` (focal 27 §15.8, step 7), then
/// flushes both directories. Only past the commit point: refused, `LogError::Identity`, while the fence
/// is of version 2. Repeated after a crash, it moves what is left. The fence, `LOCK` and `INITIALIZED`
/// stay, so the directory always says what it is.
pub fn move_segments(directory: &Path, converted: &Path) -> Result<usize, LogError> {
    let _lock = lock(directory)?;
    if !matches!(storage(directory)?, Storage::Converted { .. }) {
        return Err(LogError::Identity);
    }
    create_durable_directory(converted)?;
    let mut moved = 0usize;
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        if !is_segment(&entry.file_name()) {
            continue;
        }
        fs::rename(entry.path(), converted.join(entry.file_name()))?;
        moved = moved.saturating_add(1);
    }
    sync_dir(converted)?;
    sync_dir(directory)?;
    Ok(moved)
}

/// Removes `converted`, the segments the conversion moved there (`focal remove converted-storage`): only
/// past the commit point of `directory`, and only a directory that holds nothing but segments, so a wrong
/// path is refused rather than emptied.
pub fn remove_segments(directory: &Path, converted: &Path) -> Result<usize, LogError> {
    let _lock = lock(directory)?;
    if !matches!(storage(directory)?, Storage::Converted { .. }) {
        return Err(LogError::Identity);
    }
    if !converted.exists() {
        return Ok(0);
    }
    for entry in fs::read_dir(converted)? {
        if !is_segment(&entry?.file_name()) {
            return Err(corrupt(
                converted,
                0,
                "not a directory of converted segments",
            ));
        }
    }
    let mut removed = 0usize;
    for entry in fs::read_dir(converted)? {
        fs::remove_file(entry?.path())?;
        removed = removed.saturating_add(1);
    }
    fs::remove_dir(converted)?;
    if let Some(parent) = converted.parent() {
        sync_dir(parent)?;
    }
    Ok(removed)
}

#[cfg(test)]
#[cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects
    )
)]
#[path = "conversion_tests.rs"]
mod tests;
