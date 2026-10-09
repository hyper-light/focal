//! The upgrade fence's level as this node last knew it, kept in its data directory so that its start
//! can choose where its groups live before it opens any of them (27 §15.8, "When it runs").
//!
//! The fence is committed in the root's enrollment registry, which lives in one of the node's own
//! groups: a start that read it there would have opened its storage already. So the node's
//! controller, on observing the committed fence, records its level here, and the next start reads
//! it. A fence never closes, so the record only rises (`record` keeps the higher); a node whose
//! fence opened `STORAGE_LEVEL` while it ran converts at its next start, and one that was down
//! learns it from its root replica and converts at the start after, one restart either way.
use std::path::Path;

use crate::embedded::{NodeError, atomic_file};

/// Format: the record's file under the data directory.
pub const FILE: &str = "STORAGE.level";
/// Format: the record's magic.
const MAGIC: &[u8; 8] = b"FOCALSL1";
/// Format: the record's length: the magic, the level and the CRC-32 of both.
const LEN: usize = 8 + 4 + 4;

/// The level the node last recorded, none if it never did. A record that is there and does not
/// read whole is an error, never none: none would start the node below a fence it already passed.
pub fn known(root: &Path) -> Result<Option<u32>, NodeError> {
    let path = root.join(FILE);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let corrupt = || NodeError::Corrupt("the storage level record does not read whole");
    if bytes.len() != LEN {
        return Err(corrupt());
    }
    let (body, sum) = bytes.split_at_checked(12).ok_or_else(corrupt)?;
    let (magic, level) = body.split_at_checked(8).ok_or_else(corrupt)?;
    let sum = u32::from_le_bytes(sum.try_into().map_err(|_| corrupt())?);
    if magic != MAGIC || crc32fast::hash(body) != sum {
        return Err(corrupt());
    }
    Ok(Some(u32::from_le_bytes(
        level.try_into().map_err(|_| corrupt())?,
    )))
}

/// Records that the committed fence is at `level`, unless a higher level is recorded already: the
/// record never falls. Durable when it returns.
pub fn record(root: &Path, level: u32) -> Result<(), NodeError> {
    if known(root)?.is_some_and(|known| known >= level) {
        return Ok(());
    }
    let mut bytes = [0u8; LEN];
    let (body, sum) = bytes.split_at_mut(12);
    let (magic, at) = body.split_at_mut(8);
    magic.copy_from_slice(MAGIC);
    at.copy_from_slice(&level.to_le_bytes());
    sum.copy_from_slice(&crc32fast::hash(body).to_le_bytes());
    atomic_file(&root.join(FILE), &bytes)?;
    Ok(())
}

/// Whether the node's groups live on the shell at its start: the fence it last knew opens
/// `STORAGE_LEVEL`.
pub fn storage_opened(root: &Path) -> Result<bool, NodeError> {
    Ok(known(root)?.is_some_and(|level| level >= crate::upgrade::STORAGE_LEVEL))
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
mod tests {
    use super::*;

    /// None until recorded; a record rises and never falls; a damaged record is an error, never
    /// none; the storage level opens at `STORAGE_LEVEL`.
    #[test]
    fn the_known_level_rises_never_falls_and_is_never_read_as_absent() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(known(dir.path()).unwrap(), None);
        assert!(!storage_opened(dir.path()).unwrap());
        record(dir.path(), 3).unwrap();
        assert_eq!(known(dir.path()).unwrap(), Some(3));
        assert!(!storage_opened(dir.path()).unwrap());
        record(dir.path(), crate::upgrade::STORAGE_LEVEL).unwrap();
        assert!(storage_opened(dir.path()).unwrap());
        record(dir.path(), 2).unwrap();
        assert_eq!(
            known(dir.path()).unwrap(),
            Some(crate::upgrade::STORAGE_LEVEL)
        );
        let mut bytes = std::fs::read(dir.path().join(FILE)).unwrap();
        bytes[9] ^= 1;
        std::fs::write(dir.path().join(FILE), &bytes).unwrap();
        assert!(matches!(known(dir.path()), Err(NodeError::Corrupt(_))));
        assert!(storage_opened(dir.path()).is_err());
        std::fs::write(dir.path().join(FILE), b"short").unwrap();
        assert!(matches!(known(dir.path()), Err(NodeError::Corrupt(_))));
    }
}
