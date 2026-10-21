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
    pub fn open(
        path: impl AsRef<Path>,
        keys: &focal_seal::DataKeys,
    ) -> Result<Self, EnrollmentError> {
        let directory = PrivateDirectory::open(path.as_ref(), keys)?;
        let _ = directory.read("journal.bin")?;
        Ok(Self {
            directory,
            failed: false,
        })
    }
    /// Open an existing journal for reading beside other readers; every write
    /// is refused as `Locked`.
    pub fn open_shared(
        path: impl AsRef<Path>,
        keys: &focal_seal::DataKeys,
    ) -> Result<Self, EnrollmentError> {
        let directory = PrivateDirectory::open_shared(path.as_ref(), keys)?;
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
    /// Remember another invitation for a request whose earlier one is
    /// finished (revoked, or expired unredeemed) and retired; the ordinary
    /// `remember_invitation` stays set-once.
    pub fn replace_invitation(
        &mut self,
        request: [u8; 32],
        invitation: [u8; 16],
    ) -> Result<(), EnrollmentError> {
        if self.failed {
            return Err(EnrollmentError::Corrupt);
        }
        let name = format!("invite-{}", crate::hex(&request));
        let result = self.directory.replace(&name, &invitation);
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
        let mut journal =
            PrivateJournal::open(&path, &focal_seal::test_keys_beside(&path).unwrap()).unwrap();
        assert!(journal.read().unwrap().is_none());
        journal.replace(b"first").unwrap();
        assert!(matches!(
            PrivateJournal::open(&path, &focal_seal::test_keys_beside(&path).unwrap()),
            Err(EnrollmentError::Locked)
        ));
        journal.replace(b"second").unwrap();
        drop(journal);
        let journal =
            PrivateJournal::open(&path, &focal_seal::test_keys_beside(&path).unwrap()).unwrap();
        assert_eq!(journal.read().unwrap().unwrap().as_slice(), b"second");
        drop(journal);
        std::fs::remove_file(path.join("journal.bin")).unwrap();
        assert!(matches!(
            PrivateJournal::open(&path, &focal_seal::test_keys_beside(&path).unwrap()),
            Err(EnrollmentError::Corrupt)
        ));
    }
    /// The crash windows of a record's write, as the writer seals it (29 §12): a sealed temporary
    /// synced but not renamed is never the record, and is let go by the next writer; a record
    /// renamed into place before its initialization marker is the record, and its marker is made.
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
        let keys = focal_seal::test_keys_beside(&path).unwrap();
        let mut journal = PrivateJournal::open(&path, &keys).unwrap();
        journal.replace(b"old").unwrap();
        drop(journal);
        let mut framed = b"FCLKEY01new".to_vec();
        framed.extend_from_slice(blake3::hash(&framed).as_bytes());
        let sealed =
            focal_seal::file::seal(&keys.key(focal_seal::Store::Journal).unwrap(), &framed)
                .unwrap();
        let write_private = |at: &std::path::Path| {
            let mut file = fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .open(at)
                .unwrap();
            file.write_all(&sealed).unwrap();
            file.sync_all().unwrap();
        };
        let temporary = path.join(".pending-crash");
        write_private(&temporary);
        // Crash after temporary-file sync preserves the previous record, and the next writer lets
        // the temporary go.
        let journal = PrivateJournal::open(&path, &keys).unwrap();
        assert_eq!(journal.read().unwrap().unwrap().as_slice(), b"old");
        assert!(!temporary.exists());
        drop(journal);
        write_private(&temporary);
        fs::rename(&temporary, path.join("journal.bin")).unwrap();
        File::open(&path).unwrap().sync_all().unwrap();
        fs::remove_file(path.join("journal.bin.initialized")).unwrap();
        // Crash after installing a complete record recovers that exact record.
        let journal = PrivateJournal::open(&path, &keys).unwrap();
        assert_eq!(journal.read().unwrap().unwrap().as_slice(), b"new");
        assert!(path.join("journal.bin.initialized").exists());
    }

    /// Every file of a directory under `path`, read whole.
    fn files(path: &std::path::Path) -> Vec<(String, Vec<u8>)> {
        std::fs::read_dir(path)
            .unwrap()
            .map(|entry| entry.unwrap())
            .map(|entry| {
                (
                    entry.file_name().into_string().unwrap(),
                    std::fs::read(entry.path()).unwrap(),
                )
            })
            .collect()
    }

    /// A record as an earlier binary wrote it, in the clear: its framed bytes and its marker.
    #[cfg(unix)]
    fn legacy_record(path: &std::path::Path, name: &str, payload: &[u8]) {
        use std::os::unix::fs::OpenOptionsExt;
        let mut framed = b"FCLKEY01".to_vec();
        framed.extend_from_slice(payload);
        framed.extend_from_slice(blake3::hash(&framed).as_bytes());
        for (file, bytes) in [
            (name.to_owned(), framed),
            (format!("{name}.initialized"), Vec::new()),
        ] {
            let mut out = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .open(path.join(file))
                .unwrap();
            std::io::Write::write_all(&mut out, &bytes).unwrap();
            out.sync_all().unwrap();
        }
    }

    /// Nothing a record holds is on its volume in the clear, and the record opens under its own
    /// directory's keys only. Do: replace the journal with a marked secret, read every file; flip a
    /// byte of the record; open it under another data directory's keys. Expect: no file holds the
    /// secret, and the flipped and the foreign reads are refused, never served.
    #[test]
    fn a_record_is_sealed_at_rest_and_opens_under_its_own_keys_only() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("journal");
        let keys = focal_seal::test_keys_beside(&path).unwrap();
        let secret = b"a signer's secret, never on disk in the clear";
        let mut journal = PrivateJournal::open(&path, &keys).unwrap();
        journal.replace(secret).unwrap();
        drop(journal);
        for (name, bytes) in files(&path) {
            assert!(
                !bytes.windows(secret.len()).any(|window| window == secret),
                "{name} holds the secret"
            );
        }
        // Another data directory's keys open nothing here.
        let other = tempfile::tempdir().unwrap();
        let foreign = focal_seal::test_keys(other.path()).unwrap();
        assert!(PrivateJournal::open(&path, &foreign).is_err());
        // A flipped byte is refused.
        let record = path.join("journal.bin");
        let mut bytes = std::fs::read(&record).unwrap();
        let middle = bytes.len() / 2;
        bytes[middle] ^= 1;
        std::fs::write(&record, &bytes).unwrap();
        assert!(matches!(
            PrivateJournal::open(&path, &keys),
            Err(EnrollmentError::Corrupt)
        ));
    }

    /// A directory an earlier binary wrote in the clear is sealed by its first writer under this
    /// one, and from then on a record in the clear is refused. Do: lay out a journal and an
    /// invitation slot as plaintext records; open them shared, then as their writer; then put a
    /// plaintext record back. Expect: the shared reader reads them as they are and seals nothing;
    /// the writer reads the same records, leaves every file sealed and the directory marked; the
    /// planted plaintext is refused as corrupt.
    #[test]
    #[cfg(unix)]
    fn a_directory_written_in_the_clear_is_sealed_by_its_first_writer() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("journal");
        let keys = focal_seal::test_keys_beside(&path).unwrap();
        // A directory as an earlier binary left it: owner-only, its lock, two records.
        focal_platform::fs::ensure_dir_private(&path).unwrap();
        legacy_record(&path, "journal.bin", b"the journal");
        let request = [7u8; 32];
        legacy_record(
            &path,
            &format!("invite-{}", crate::hex(&request)),
            &[9u8; 16],
        );
        let shared = PrivateJournal::open_shared(&path, &keys).unwrap();
        assert_eq!(shared.read().unwrap().unwrap().as_slice(), b"the journal");
        assert_eq!(shared.invitation(request).unwrap(), Some([9u8; 16]));
        drop(shared);
        assert!(!path.join("SEALED").exists(), "a reader seals nothing");
        let journal = PrivateJournal::open(&path, &keys).unwrap();
        assert_eq!(journal.read().unwrap().unwrap().as_slice(), b"the journal");
        assert_eq!(journal.invitation(request).unwrap(), Some([9u8; 16]));
        drop(journal);
        assert!(path.join("SEALED").exists());
        for (name, bytes) in files(&path) {
            assert!(
                !bytes.starts_with(b"FCLKEY01"),
                "{name} is still in the clear"
            );
        }
        std::fs::remove_file(path.join("journal.bin")).unwrap();
        std::fs::remove_file(path.join("journal.bin.initialized")).unwrap();
        legacy_record(&path, "journal.bin", b"planted");
        assert!(matches!(
            PrivateJournal::open(&path, &keys),
            Err(EnrollmentError::Corrupt)
        ));
    }

    #[test]
    fn shared_readers_coexist_and_exclude_writers_without_writing() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("journal");
        assert!(matches!(
            PrivateJournal::open_shared(&path, &focal_seal::test_keys_beside(&path).unwrap()),
            Err(EnrollmentError::Invalid)
        ));
        let mut journal =
            PrivateJournal::open(&path, &focal_seal::test_keys_beside(&path).unwrap()).unwrap();
        journal.replace(b"saved").unwrap();
        drop(journal);
        let mut first =
            PrivateJournal::open_shared(&path, &focal_seal::test_keys_beside(&path).unwrap())
                .unwrap();
        let second =
            PrivateJournal::open_shared(&path, &focal_seal::test_keys_beside(&path).unwrap())
                .unwrap();
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
            PrivateJournal::open(&path, &focal_seal::test_keys_beside(&path).unwrap()),
            Err(EnrollmentError::Locked)
        ));
        drop(first);
        assert!(matches!(
            PrivateJournal::open(&path, &focal_seal::test_keys_beside(&path).unwrap()),
            Err(EnrollmentError::Locked)
        ));
        drop(second);
        let mut writer =
            PrivateJournal::open(&path, &focal_seal::test_keys_beside(&path).unwrap()).unwrap();
        writer.replace(b"changed").unwrap();
        // A reader is refused while a writer holds the directory.
        assert!(matches!(
            PrivateJournal::open_shared(&path, &focal_seal::test_keys_beside(&path).unwrap()),
            Err(EnrollmentError::Locked)
        ));
        drop(writer);
        assert_eq!(
            PrivateJournal::open_shared(&path, &focal_seal::test_keys_beside(&path).unwrap())
                .unwrap()
                .read()
                .unwrap()
                .unwrap()
                .as_slice(),
            b"changed"
        );
    }
}
