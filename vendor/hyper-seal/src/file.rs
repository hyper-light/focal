//! A root key from a file (`docs/seal.md` §3.2): 32 bytes in a file the configuration names, off the
//! data devices, readable by its owner alone. The crates here are sans-io, so the consumer's device
//! module creates and reads the file; this checks the open file's permissions (its mode on Unix,
//! its DACL on Windows) and the length of what was read.

use std::fs::File;

use crate::keys::{KeyId, KeySource, Wrapped, WrappingKey};
use crate::{SealError, Secret32};

/// A root key read from an owner-only file.
pub struct FileSource {
    key: WrappingKey,
}

impl FileSource {
    /// The root key in `bytes`, read from `file`, with the ID and generation the consumer records for
    /// it. A file any other principal may read, or of any length but 32, is refused.
    pub fn new(id: KeyId, generation: u32, bytes: &[u8], file: &File) -> Result<Self, SealError> {
        owner_only(file)?;
        let bytes: &[u8; 32] = bytes
            .try_into()
            .map_err(|_| SealError::Source("a key file of other than 32 bytes"))?;
        Ok(Self {
            key: WrappingKey::new(id, generation, Secret32::from_bytes(bytes)?),
        })
    }
}

/// Whether only the file's owner may read or write it: no permission bit for its group or others.
#[cfg(unix)]
fn owner_only(file: &File) -> Result<(), SealError> {
    use std::os::unix::fs::PermissionsExt as _;
    let metadata = file
        .metadata()
        .map_err(|_| SealError::Source("the key file's metadata could not be read"))?;
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(SealError::Source("a key file another principal may read"));
    }
    Ok(())
}

/// On Windows the owner-only check reads the file's DACL.
#[cfg(windows)]
fn owner_only(file: &File) -> Result<(), SealError> {
    crate::file_windows::owner_only(file)
}

impl KeySource for FileSource {
    fn id(&self) -> (KeyId, u32) {
        (self.key.id(), self.key.generation())
    }

    fn wrap(&mut self, key: &Secret32) -> Result<Wrapped, SealError> {
        self.key.wrap(key)
    }

    fn unwrap(&mut self, wrapped: &Wrapped) -> Result<Secret32, SealError> {
        self.key.unwrap(wrapped)
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt as _;

    /// A key file in a directory the test owns, at `mode`.
    fn key_file(mode: u32) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("root.key");
        #[allow(clippy::disallowed_methods)]
        {
            std::fs::write(&path, [3u8; 32]).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
        }
        (dir, path)
    }

    #[test]
    fn an_owner_only_file_is_a_source() {
        let (_dir, path) = key_file(0o600);
        let bytes = std::fs::read(&path).unwrap();
        let mut source =
            FileSource::new(KeyId([1; 16]), 0, &bytes, &File::open(&path).unwrap()).unwrap();
        let child = crate::random_secret().unwrap();
        let wrapped = source.wrap(&child).unwrap();
        assert_eq!(source.unwrap(&wrapped).unwrap().bytes(), child.bytes());
        // Read-only for its owner, as an operator's file may be: still a source.
        let (_dir, path) = key_file(0o400);
        assert!(FileSource::new(KeyId([1; 16]), 0, &bytes, &File::open(&path).unwrap()).is_ok());
    }

    #[test]
    fn a_file_others_may_read_is_refused() {
        for mode in [0o640, 0o604, 0o644, 0o660, 0o606] {
            let (_dir, path) = key_file(mode);
            let bytes = std::fs::read(&path).unwrap();
            assert_eq!(
                FileSource::new(KeyId([1; 16]), 0, &bytes, &File::open(&path).unwrap()).err(),
                Some(SealError::Source("a key file another principal may read")),
                "mode {mode:o}"
            );
        }
    }

    #[test]
    fn a_file_of_another_length_is_refused() {
        let (_dir, path) = key_file(0o600);
        let meta = File::open(&path).unwrap();
        assert!(FileSource::new(KeyId([1; 16]), 0, &[0; 31], &meta).is_err());
        assert!(FileSource::new(KeyId([1; 16]), 0, &[0; 33], &meta).is_err());
    }
}
