//! Fixed-name, locked private retry state for the node's enrollment signer.
use crate::{EnrollmentError, files::PrivateDirectory};
use std::path::Path;
use zeroize::Zeroizing;

/// One atomic state record, at most 60 KiB, in an owner-only directory. An
/// initialization marker makes a missing record an error on subsequent opens.
/// Contents have no Debug surface and reads zeroize their temporary byte buffer.
pub struct PrivateJournal {
    directory: PrivateDirectory,
    failed: bool,
}
impl PrivateJournal {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, EnrollmentError> {
        let directory = PrivateDirectory::open(path.as_ref())?;
        let _ = directory.read("journal.bin")?;
        Ok(Self {
            directory,
            failed: false,
        })
    }
    /// Open an existing journal for reading beside other readers; every write
    /// is refused as `Locked`.
    pub fn open_shared(path: impl AsRef<Path>) -> Result<Self, EnrollmentError> {
        let directory = PrivateDirectory::open_shared(path.as_ref())?;
        let _ = directory.read("journal.bin")?;
        Ok(Self {
            directory,
            failed: false,
        })
    }
    pub fn read(&self) -> Result<Option<Zeroizing<Vec<u8>>>, EnrollmentError> {
        if self.failed {
            return Err(EnrollmentError::Corrupt);
        }
        self.directory.read("journal.bin")
    }
    /// A durable parent record detects loss of an entire invitation draft
    /// directory, not just loss of its secret file. No secret is stored here.
    pub fn invitation(&self, request: [u8; 32]) -> Result<Option<[u8; 16]>, EnrollmentError> {
        if self.failed {
            return Err(EnrollmentError::Corrupt);
        }
        let name = format!("invite-{}", crate::hex(&request));
        self.directory
            .read(&name)?
            .map(|bytes| {
                bytes
                    .as_slice()
                    .try_into()
                    .map_err(|_| EnrollmentError::Corrupt)
            })
            .transpose()
    }
    pub fn remember_invitation(
        &mut self,
        request: [u8; 32],
        invitation: [u8; 16],
    ) -> Result<(), EnrollmentError> {
        match self.invitation(request)? {
            Some(existing) if existing == invitation => return Ok(()),
            Some(_) => return Err(EnrollmentError::Conflict),
            None => {}
        }
        let name = format!("invite-{}", crate::hex(&request));
        let result = self.directory.install_new(&name, &invitation);
        // A refused write on a shared handle changed nothing; only a failed
        // write leaves the record in doubt.
        if result.is_err() && !matches!(result, Err(EnrollmentError::Locked)) {
            self.failed = true;
        }
        result
    }
    pub fn replace(&mut self, bytes: &[u8]) -> Result<(), EnrollmentError> {
        if self.failed {
            return Err(EnrollmentError::Corrupt);
        }
        if bytes.len() > 60 * 1024 {
            return Err(EnrollmentError::Capacity);
        }
        let result = self.directory.replace("journal.bin", bytes);
        if result.is_err() && !matches!(result, Err(EnrollmentError::Locked)) {
            self.failed = true;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn replace_recovery_lock_and_lost_record_are_fail_closed() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("journal");
        let mut journal = PrivateJournal::open(&path).unwrap();
        assert!(journal.read().unwrap().is_none());
        journal.replace(b"first").unwrap();
        assert!(matches!(
            PrivateJournal::open(&path),
            Err(EnrollmentError::Locked)
        ));
        journal.replace(b"second").unwrap();
        drop(journal);
        let journal = PrivateJournal::open(&path).unwrap();
        assert_eq!(journal.read().unwrap().unwrap().as_slice(), b"second");
        drop(journal);
        std::fs::remove_file(path.join("journal.bin")).unwrap();
        assert!(matches!(
            PrivateJournal::open(&path),
            Err(EnrollmentError::Corrupt)
        ));
    }
    #[test]
    #[cfg(unix)]
    fn orphaned_temp_is_uncommitted_and_rename_recovers_initial_marker_window() {
        use std::{
            fs::{self, File},
            io::Write,
            os::unix::fs::OpenOptionsExt,
        };
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("journal");
        let mut journal = PrivateJournal::open(&path).unwrap();
        journal.replace(b"old").unwrap();
        drop(journal);
        let mut framed = b"FCLKEY01new".to_vec();
        framed.extend_from_slice(blake3::hash(&framed).as_bytes());
        let temporary = path.join(".pending-crash");
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&temporary)
            .unwrap();
        file.write_all(&framed).unwrap();
        file.sync_all().unwrap();
        drop(file);
        // Crash after temporary-file sync preserves the previous record.
        let journal = PrivateJournal::open(&path).unwrap();
        assert_eq!(journal.read().unwrap().unwrap().as_slice(), b"old");
        drop(journal);
        fs::rename(&temporary, path.join("journal.bin")).unwrap();
        File::open(&path).unwrap().sync_all().unwrap();
        fs::remove_file(path.join("journal.bin.initialized")).unwrap();
        // Crash after installing a complete record recovers that exact record.
        let journal = PrivateJournal::open(&path).unwrap();
        assert_eq!(journal.read().unwrap().unwrap().as_slice(), b"new");
        assert!(path.join("journal.bin.initialized").exists());
    }

    #[test]
    fn shared_readers_coexist_and_exclude_writers_without_writing() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("journal");
        assert!(matches!(
            PrivateJournal::open_shared(&path),
            Err(EnrollmentError::Invalid)
        ));
        let mut journal = PrivateJournal::open(&path).unwrap();
        journal.replace(b"saved").unwrap();
        drop(journal);
        let mut first = PrivateJournal::open_shared(&path).unwrap();
        let second = PrivateJournal::open_shared(&path).unwrap();
        assert_eq!(first.read().unwrap().unwrap().as_slice(), b"saved");
        assert_eq!(second.read().unwrap().unwrap().as_slice(), b"saved");
        // A shared owner never writes, and a writer waits for every reader.
        assert!(matches!(
            first.replace(b"changed"),
            Err(EnrollmentError::Locked)
        ));
        assert!(matches!(
            first.remember_invitation([1; 32], [2; 16]),
            Err(EnrollmentError::Locked)
        ));
        assert!(matches!(
            PrivateJournal::open(&path),
            Err(EnrollmentError::Locked)
        ));
        drop(first);
        assert!(matches!(
            PrivateJournal::open(&path),
            Err(EnrollmentError::Locked)
        ));
        drop(second);
        let mut writer = PrivateJournal::open(&path).unwrap();
        writer.replace(b"changed").unwrap();
        // A reader is refused while a writer holds the directory.
        assert!(matches!(
            PrivateJournal::open_shared(&path),
            Err(EnrollmentError::Locked)
        ));
        drop(writer);
        assert_eq!(
            PrivateJournal::open_shared(&path)
                .unwrap()
                .read()
                .unwrap()
                .unwrap()
                .as_slice(),
            b"changed"
        );
    }
}
