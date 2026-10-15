//! Scratch files a measurement writes and removes (audit S06).
//!
//! A scratch file is created under a name no file had, with exclusive creation (`O_CREAT |
//! O_EXCL` on POSIX, `CREATE_NEW` on Windows), which fails on any file or link already at the
//! name rather than opening it, so a measurement never writes into a file it did not create. Its
//! removal is armed only once that creation succeeded, so a measurement that fails never removes
//! a file it did not create. The name carries 64 bits from the operating system's random source:
//! no other process or run chooses it, and none can place a file there first but by chance.

use std::fs::OpenOptions;
use std::io;
use std::path::{Path, PathBuf};

use crate::DiskError;

/// A file this process created, removed when dropped.
#[derive(Debug)]
pub struct Scratch {
    path: PathBuf,
}

impl Scratch {
    /// Creates an empty file in `dir` named `prefix`, a dash, and 64 random bits in hex.
    pub fn create(dir: &Path, prefix: &str) -> Result<Self, DiskError> {
        let mut bits = [0u8; 8];
        getrandom::fill(&mut bits).map_err(|_| DiskError::Io {
            op: "draw a scratch file's name",
            path: dir.to_path_buf(),
            source: io::Error::other("the operating system's random source failed"),
        })?;
        Self::at(dir.join(format!("{prefix}-{:016x}", u64::from_le_bytes(bits))))
    }

    /// Creates the empty file `path`, which must not exist.
    fn at(path: PathBuf) -> Result<Self, DiskError> {
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(_) => Ok(Self { path }),
            Err(source) => Err(DiskError::Io {
                op: "create a scratch file",
                path,
                source,
            }),
        }
    }

    /// Where the file is.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        // Nothing to report to: the file is ours, and absent is the goal.
        #[expect(
            clippy::disallowed_methods,
            reason = "a scratch file is removed by what created it, and by nothing else"
        )]
        let _ = std::fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A scratch file is made new and removed when dropped.
    #[test]
    fn a_scratch_file_is_new_and_goes_when_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let s = Scratch::create(dir.path(), ".probe").unwrap();
        let path = s.path().to_path_buf();
        assert!(path.exists());
        assert!(
            path.file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with(".probe-")
        );
        let other = Scratch::create(dir.path(), ".probe").unwrap();
        assert_ne!(other.path(), s.path());
        drop(s);
        assert!(!path.exists());
        assert!(other.path().exists());
    }

    /// A file or a link already at the name is refused, and left as it was: a scratch file
    /// never writes or removes what it did not create.
    #[test]
    fn what_is_already_at_the_name_is_refused_and_kept() {
        let dir = tempfile::tempdir().unwrap();
        let taken = dir.path().join("taken");
        #[expect(
            clippy::disallowed_methods,
            reason = "the test puts a file where a scratch file would go"
        )]
        std::fs::write(&taken, b"customer-data").unwrap();
        let refused = Scratch::at(taken.clone());
        assert!(matches!(
            refused,
            Err(DiskError::Io { ref source, .. }) if source.kind() == io::ErrorKind::AlreadyExists
        ));
        drop(refused);
        assert_eq!(std::fs::read(&taken).unwrap(), b"customer-data");

        #[cfg(unix)]
        {
            let link = dir.path().join("link");
            std::os::unix::fs::symlink(&taken, &link).unwrap();
            assert!(Scratch::at(link.clone()).is_err());
            assert!(link.symlink_metadata().is_ok());
            assert_eq!(std::fs::read(&taken).unwrap(), b"customer-data");
        }
    }
}
