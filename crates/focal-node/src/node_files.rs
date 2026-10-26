//! A node's small files at rest (29 §12.10): its identity, policy and network state, its founded
//! allowance and adopted trust, its partition records, its root's genesis, its deployment plans and
//! journals, and the retirements it has to finish. Each is written whole through the atomic replace
//! every node file is written by ([`crate::embedded::atomic_file`]): in the clear, byte for byte as
//! before, below the at-rest level (`upgrade::AT_REST_LEVEL`), where a binary below it may still be
//! rolled back to; from the level, sealed whole under the data directory's journal key
//! (`Store::Journal`, `focal_seal::file`), the file's name bound inside the seal, so a file moved to
//! another name, or carried from another directory, is refused as a changed byte is.
//!
//! A directory whose fence rose to the level is converted at its node's start, under the directory's
//! lock and before the node serves ([`NodeFiles::convert`]): each file in the clear is read and
//! checked by the rule its own reader holds it to, written sealed through its atomic replace, and the
//! directory is then marked ([`SEALED`]); from the mark a file in the clear is refused. A cut anywhere
//! leaves each file whole in one form or the other, and the next start finishes. Before the mark a
//! reader takes either form: a node whose fence rose while it ran seals what it writes from then on
//! and converts the rest at its next start.
//!
//! A sealed file begins with [`SEALED_MAGIC`] in the clear, which says what the file is and nothing
//! of what it holds, so either form is told from the file alone.

use std::path::{Path, PathBuf};

use focal_seal::{DataKeys, Store};
use hyper_seal::keys::WrappingKey;

use crate::embedded::{NodeError, atomic_file};

/// Format: the marker of a data directory whose node files are all sealed: from it a node file in the
/// clear is refused. An empty file, in the clear by design (29 §12).
pub const SEALED: &str = "FILES.SEALED";

/// Format: what a sealed node file begins with, in the clear: its kind, and nothing of what it holds.
pub const SEALED_MAGIC: &[u8; 8] = b"FCLNSEAL";

/// Format: what a sealed node file's plaintext begins with, then its name's length (two bytes,
/// little-endian), its name relative to the data directory (`/` between its parts), and the file's
/// bytes as its writer made them.
const FRAME: &[u8; 8] = b"FCLNFIL1";

/// Derived: the bytes of a frame before its name: the frame's magic and the name's length.
const FRAME_HEAD: usize = FRAME.len() + 2;

/// A node's small files, as its data directory keeps them: sealed under its keys once the fence it
/// last knew opens the at-rest level, in the clear below it. The form is read at each write, from
/// the fence the directory records (`STORAGE.level`), so a node whose fence rose as it ran seals
/// what it writes from then on.
#[derive(Clone, Debug)]
pub struct NodeFiles {
    root: PathBuf,
    /// The directory's keys, which every sealed file opens under; none for a directory that has no
    /// keys yet, whose files are all in the clear.
    keys: Option<DataKeys>,
}

impl NodeFiles {
    /// The node files of the data directory `root`, whose keys are `keys`.
    pub fn new(root: &Path, keys: Option<DataKeys>) -> Self {
        Self {
            root: root.to_path_buf(),
            keys,
        }
    }

    /// Whether a file written now is sealed: the fence the directory records opens the at-rest
    /// level.
    fn sealing(&self) -> Result<bool, NodeError> {
        Ok(crate::storage_level::at_rest_form(&self.root)? == focal_evidence::ContentForm::Sealed)
    }

    /// The data directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Whether the directory is marked: every node file in it sealed, and a file in the clear
    /// refused. A mark that cannot be looked for is an error, never its absence.
    pub fn marked(&self) -> Result<bool, NodeError> {
        Ok(self.root.join(SEALED).try_exists()?)
    }

    /// Writes `bytes` as the node file `name` (relative to the data directory), whole, through the
    /// atomic replace: sealed from the at-rest level, in the clear below it.
    pub fn write(&self, name: &str, bytes: &[u8]) -> Result<(), NodeError> {
        let path = self.path(name)?;
        if self.sealing()? {
            atomic_file(&path, &seal(&self.journal_key()?, name, bytes)?)?;
        } else {
            atomic_file(&path, bytes)?;
        }
        Ok(())
    }

    /// The node file `name`'s bytes as its writer made them, no more than `most` of them; none where
    /// it does not exist. A file sealed is opened under the directory's keys, its name checked; one in
    /// the clear is taken only before the directory's mark.
    pub fn read(&self, name: &str, most: usize) -> Result<Option<Vec<u8>>, NodeError> {
        let path = self.path(name)?;
        if !path.try_exists()? {
            return Ok(None);
        }
        let bytes = crate::embedded::read_bounded(&path, on_disk_bound(name, most)?)?;
        if bytes.starts_with(SEALED_MAGIC) {
            return open(&self.journal_key()?, name, bytes, most).map(Some);
        }
        if self.marked()? {
            return Err(NodeError::ClearAfterSealed(name.to_owned()));
        }
        if bytes.len() > most {
            return Err(NodeError::PastBound { path, most });
        }
        Ok(Some(bytes))
    }

    /// The key a node file is sealed under: the directory's journal key, unwrapped for the write or
    /// read, or the conversion, that takes it and dropped after. A directory that has no keys seals
    /// nothing, and a node file sealed in one is refused.
    fn journal_key(&self) -> Result<WrappingKey, NodeError> {
        let keys = self.keys.as_ref().ok_or(NodeError::SealedWithoutKeys)?;
        Ok(keys.key(Store::Journal)?)
    }

    /// `name`'s path in the data directory: a name of parts, none empty, `.` or `..`.
    fn path(&self, name: &str) -> Result<PathBuf, NodeError> {
        let mut path = self.root.clone();
        for part in name.split('/') {
            if part.is_empty() || part == "." || part == ".." {
                return Err(NodeError::Corrupt("a node file's name"));
            }
            path.push(part);
        }
        Ok(path)
    }
}

/// The identity of the node whose data directory is `root`, read beside its running owner or with
/// none running: its node files opened under the keys `settings` finds for it, made nowhere, the
/// identity in either form they take. What a command line reads a node's identity with.
pub fn read_identity(
    settings: &crate::config::Settings,
    root: &Path,
) -> Result<crate::embedded::NodeIdentity, NodeError> {
    let files = NodeFiles::new(root, settings.found_data_keys(root)?);
    let bytes = files
        .read("IDENTITY", crate::embedded::IDENTITY_BYTES)?
        .ok_or(NodeError::Identity)?;
    crate::embedded::decode_identity_bytes(&bytes)
}

/// The most bytes a frame of `name` holding `most` bytes takes.
fn frame_len(name: &str, most: usize) -> Result<usize, NodeError> {
    FRAME_HEAD
        .checked_add(name.len())
        .and_then(|head| head.checked_add(most))
        .ok_or(NodeError::Capacity("a node file's bound"))
}

/// The most bytes the node file `name` holding `most` bytes takes on disk, in either form: its
/// bytes in the clear, or the clear magic and the seal of its frame.
pub(crate) fn on_disk_bound(name: &str, most: usize) -> Result<usize, NodeError> {
    focal_seal::file::sealed_bound(frame_len(name, most)?)
        .and_then(|sealed| sealed.checked_add(SEALED_MAGIC.len()))
        .map(|sealed| sealed.max(most))
        .ok_or(NodeError::Capacity("a node file's bound"))
}

/// `bytes`, the node file `name`, sealed whole under the directory's journal key: the clear magic,
/// then the seal of its frame.
fn seal(key: &WrappingKey, name: &str, bytes: &[u8]) -> Result<Vec<u8>, NodeError> {
    let name_len =
        u16::try_from(name.len()).map_err(|_| NodeError::Capacity("a node file's name"))?;
    let mut frame = zeroize::Zeroizing::new(Vec::new());
    frame
        .try_reserve_exact(frame_len(name, bytes.len())?)
        .map_err(|_| NodeError::Capacity("a node file's frame"))?;
    frame.extend_from_slice(FRAME);
    frame.extend_from_slice(&name_len.to_le_bytes());
    frame.extend_from_slice(name.as_bytes());
    frame.extend_from_slice(bytes);
    let sealed = focal_seal::file::seal(key, &frame)?;
    let mut out = Vec::new();
    out.try_reserve_exact(SEALED_MAGIC.len().saturating_add(sealed.len()))
        .map_err(|_| NodeError::Capacity("a sealed node file"))?;
    out.extend_from_slice(SEALED_MAGIC);
    out.extend_from_slice(&sealed);
    Ok(out)
}

/// The bytes the sealed node file `bytes`, read as `name`, holds: opened under the directory's journal
/// key, its frame's name checked, no more than `most` of them.
fn open(key: &WrappingKey, name: &str, bytes: Vec<u8>, most: usize) -> Result<Vec<u8>, NodeError> {
    let malformed = || NodeError::Corrupt("a sealed node file's frame");
    let mut sealed = bytes;
    if !sealed.starts_with(SEALED_MAGIC) {
        return Err(malformed());
    }
    sealed.drain(..SEALED_MAGIC.len());
    let frame = focal_seal::file::open(key, sealed, frame_len(name, most)?)?;
    let (magic, rest) = frame.split_at_checked(FRAME.len()).ok_or_else(malformed)?;
    let (stated, rest) = rest.split_at_checked(2).ok_or_else(malformed)?;
    let stated = usize::from(u16::from_le_bytes(
        stated.try_into().map_err(|_| malformed())?,
    ));
    let (named, body) = rest.split_at_checked(stated).ok_or_else(malformed)?;
    if magic != FRAME || named != name.as_bytes() {
        return Err(NodeError::MovedNodeFile(name.to_owned()));
    }
    if body.len() > most {
        return Err(malformed());
    }
    Ok(body.to_vec())
}

/// Derived: the partition records a data directory holds at most: one for each partition its node
/// may host (`deployment::plan::MAX_PARTITIONS`); more is a directory that is not this node's.
const MOST_PARTITION_RECORDS: usize = crate::deployment::plan::MAX_PARTITIONS;

/// What a conversion did: the files it sealed and those it found sealed already.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Converted {
    pub sealed: usize,
    pub found_sealed: usize,
}

/// A node file the conversion visits: the most bytes it holds, and the rule its own reader holds it
/// to.
#[derive(Clone, Copy)]
struct Kind {
    most: usize,
    check: fn(&[u8]) -> Result<(), NodeError>,
}

/// The node files at fixed names, each with its kind.
fn fixed() -> [(&'static str, Kind); 6] {
    [
        (
            "IDENTITY",
            Kind {
                most: crate::embedded::IDENTITY_BYTES,
                check: crate::embedded::check_identity,
            },
        ),
        (
            "POLICY",
            Kind {
                most: crate::config::policy::MAX_BYTES,
                check: crate::config::policy::check,
            },
        ),
        (
            "NETWORK",
            Kind {
                most: crate::network_state::MAX_BYTES,
                check: crate::network_state::check,
            },
        ),
        (
            "SESSION.memory",
            Kind {
                most: crate::memory_envelope::FOUNDED_BYTES,
                check: crate::memory_envelope::check,
            },
        ),
        (
            "cluster/BOOTSTRAP",
            Kind {
                most: crate::cluster::GENESIS_BYTES,
                check: crate::cluster::check_genesis,
            },
        ),
        (
            "cluster/pending-retires",
            Kind {
                most: crate::placement_agent::PENDING_RETIRES_BYTES,
                check: crate::placement_agent::check_pending_retires,
            },
        ),
    ]
}

impl NodeFiles {
    /// Converts every node file of the directory in the clear to sealed, then marks it: what the
    /// node's start does under the directory's lock, before it serves, once its fence opens the
    /// at-rest level. Each file in the clear is checked by its reader's rule and written sealed
    /// through the atomic replace that writes it; one sealed already (by a conversion a cut ended,
    /// or written since the fence rose) is opened to check it and left; what a cut install left
    /// beside a file (its temporary) is removed. A cut anywhere leaves each file whole in one form
    /// or the other, and the next start finishes. Below the level, or marked already, nothing is
    /// done. The files are visited as they are listed, none held but the one being converted.
    pub fn convert(&self) -> Result<Converted, NodeError> {
        if self.marked()? || !self.sealing()? {
            return Ok(Converted::default());
        }
        let key = self.journal_key()?;
        let mut converted = Converted::default();
        for (name, kind) in fixed() {
            self.convert_one(&key, name, kind, &mut converted)?;
        }
        let records = Kind {
            most: crate::network_service::network_directory::RECORD_BYTES,
            check: crate::network_service::network_directory::check_record,
        };
        let mut listed = 0usize;
        for name in names(&self.root.join("cluster/partitions"))? {
            let name = name?;
            if !name.ends_with(".partition") {
                continue;
            }
            listed = listed.saturating_add(1);
            if listed > MOST_PARTITION_RECORDS {
                return Err(NodeError::Corrupt(
                    "more partition records than a node hosts",
                ));
            }
            self.convert_one(
                &key,
                &format!("cluster/partitions/{name}"),
                records,
                &mut converted,
            )?;
        }
        let plan = Kind {
            most: crate::deployment::plan::MAX_PLAN_BYTES,
            check: crate::deployment::apply::check_plan,
        };
        let journal = Kind {
            most: crate::deployment::apply::MAX_JOURNAL_BYTES,
            check: crate::deployment::apply::check_journal,
        };
        // Every plan the directory holds: one left in the clear would be refused once marked. (The
        // plans a node keeps are not pruned: an open defect, 29 §12.10.)
        for name in names(&self.root.join("cluster/apply"))? {
            let name = name?;
            self.convert_one(
                &key,
                &format!("cluster/apply/{name}/PLAN"),
                plan,
                &mut converted,
            )?;
            self.convert_one(
                &key,
                &format!("cluster/apply/{name}/JOURNAL"),
                journal,
                &mut converted,
            )?;
        }
        // The issuers each client enrollment of the directory adopted (its contexts' catalogue),
        // and what an earlier binary's cut adoption left in the clear beside them.
        let adopted = Kind {
            most: crate::network_join::MAX_ADOPTED_BYTES,
            check: crate::network_join::check_adopted,
        };
        let mut enrollments = 0usize;
        for name in names(&self.root.join("CLIENT.contexts"))? {
            let name = name?;
            if !name.starts_with("enrollment-") {
                continue;
            }
            enrollments = enrollments.saturating_add(1);
            if enrollments > crate::network_join::MAX_CONTEXTS {
                return Err(NodeError::Corrupt("more client enrollments than contexts"));
            }
            let staged = self
                .path(&format!("CLIENT.contexts/{name}"))?
                .join(crate::network_join::ADOPTED_TEMPORARY);
            match std::fs::remove_file(&staged) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            self.convert_one(
                &key,
                &format!("CLIENT.contexts/{name}/trust-adopted.bin"),
                adopted,
                &mut converted,
            )?;
        }
        atomic_file(&self.root.join(SEALED), b"")?;
        Ok(converted)
    }

    /// Converts the node file `name` of kind `kind`, where it is in the clear.
    fn convert_one(
        &self,
        key: &WrappingKey,
        name: &str,
        kind: Kind,
        converted: &mut Converted,
    ) -> Result<(), NodeError> {
        let path = self.path(name)?;
        discard_temporary(&path)?;
        if !path.try_exists()? {
            return Ok(());
        }
        let bytes = crate::embedded::read_bounded(&path, on_disk_bound(name, kind.most)?)?;
        if bytes.starts_with(SEALED_MAGIC) {
            (kind.check)(&open(key, name, bytes, kind.most)?)?;
            converted.found_sealed = converted.found_sealed.saturating_add(1);
            return Ok(());
        }
        if bytes.len() > kind.most {
            return Err(NodeError::PastBound {
                path,
                most: kind.most,
            });
        }
        (kind.check)(&bytes)?;
        atomic_file(&path, &seal(key, name, &bytes)?)?;
        converted.sealed = converted.sealed.saturating_add(1);
        Ok(())
    }
}

/// The names in `directory`, each as it is listed; none where it does not exist.
fn names(directory: &Path) -> Result<impl Iterator<Item = Result<String, NodeError>>, NodeError> {
    let entries = match std::fs::read_dir(directory) {
        Ok(entries) => Some(entries),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    Ok(entries
        .into_iter()
        .flatten()
        .filter_map(|entry| match entry {
            Ok(entry) => entry.file_name().to_str().map(|name| Ok(name.to_owned())),
            Err(error) => Some(Err(NodeError::from(error))),
        }))
}

/// Removes what a cut install of `path` left: its temporary, the name the atomic replace writes it
/// through ([`atomic_file`]).
fn discard_temporary(path: &Path) -> Result<(), NodeError> {
    match std::fs::remove_file(path.with_extension("install")) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A data directory with keys, its fence recorded at `level`.
    fn directory(level: u32) -> (tempfile::TempDir, NodeFiles) {
        let dir = tempfile::tempdir().unwrap();
        let keys = focal_seal::test_keys(dir.path()).unwrap();
        crate::storage_level::record(dir.path(), level).unwrap();
        let files = NodeFiles::new(dir.path(), Some(keys));
        (dir, files)
    }

    /// An identity file's bytes, as a node's directory writes them.
    fn identity_bytes() -> Vec<u8> {
        let identity = crate::embedded::new_identity().unwrap();
        let payload = postcard::to_stdvec(&identity).unwrap();
        let mut bytes = b"FOCALND1".to_vec();
        bytes.extend_from_slice(blake3::hash(&payload).as_bytes());
        bytes.extend_from_slice(&payload);
        bytes
    }

    /// Plants, below the at-rest level, a node file of each kind a test can make whole: the identity,
    /// the committed policy, the founded allowance, the pending retirements, a deployment's plan and
    /// journal, a partition record, the root's genesis, and the issuers a client enrollment adopted.
    /// Their names and bytes.
    fn plant(files: &NodeFiles) -> Vec<(String, Vec<u8>)> {
        let planted = vec![
            ("IDENTITY", identity_bytes()),
            ("SESSION.memory", b"1073741824\n".to_vec()),
            ("cluster/pending-retires", [7u8; 32].to_vec()),
        ];
        std::fs::create_dir_all(files.root().join("cluster")).unwrap();
        for (name, bytes) in &planted {
            files.write(name, bytes).unwrap();
        }
        crate::config::policy::install_or_check(files, &crate::config::PolicyIntent::default())
            .unwrap();
        let policy = std::fs::read(files.root().join("POLICY")).unwrap();
        let mut planted: Vec<(String, Vec<u8>)> = planted
            .into_iter()
            .map(|(name, bytes)| (name.to_owned(), bytes))
            .collect();
        planted.push(("POLICY".to_owned(), policy));
        // A deployment's plan and journal, as its apply keeps them.
        let (plan, journal) = crate::deployment::tests::plan_and_journal();
        let id: String = plan
            .plan_id
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        std::fs::create_dir_all(files.root().join(format!("cluster/apply/{id}"))).unwrap();
        for (file, bytes) in [
            ("PLAN", plan.encode().unwrap()),
            ("JOURNAL", journal.encode().unwrap()),
        ] {
            let name = format!("cluster/apply/{id}/{file}");
            files.write(&name, &bytes).unwrap();
            planted.push((name, bytes));
        }
        // A partition record, as hosting writes one.
        let record = crate::network_service::network_directory::tests::plant_record(files);
        let bytes = std::fs::read(files.root().join(&record)).unwrap();
        planted.push((record, bytes));
        // The root's genesis, as its enrollment writes it.
        let genesis = crate::cluster::tests::plant_genesis(files);
        let bytes = std::fs::read(files.root().join(&genesis)).unwrap();
        planted.push((genesis, bytes));
        // The issuers a client enrollment of the directory adopted, as its join keeps them.
        let enrollment = files.root().join("CLIENT.contexts/enrollment-alice");
        std::fs::create_dir_all(&enrollment).unwrap();
        let authority = files.root().join("planted-authority");
        let authority = focal_enrollment::BootstrapAuthority::open_or_create(
            &authority,
            &focal_seal::test_keys_beside(&authority).unwrap(),
            [9; 16],
            vec!["localhost".into()],
            1_783_000_000,
        )
        .unwrap();
        let adopted = focal_wire::Adopted {
            anchor: authority.ca_certificate().to_vec(),
            issuer: None,
        };
        let keys = files.keys.as_ref().unwrap();
        assert!(crate::network_join::adopt_issuer(keys, &enrollment, &adopted).unwrap());
        let name = "CLIENT.contexts/enrollment-alice/trust-adopted.bin".to_owned();
        let bytes = std::fs::read(files.root().join(&name)).unwrap();
        planted.push((name, bytes));
        planted
    }

    /// Below the at-rest level a node file is written as every earlier binary wrote it, byte for
    /// byte; from the level, sealed, its bytes nowhere in the clear, and read back whole.
    #[test]
    fn a_node_file_is_written_as_today_below_the_level_and_sealed_from_it() {
        let (dir, files) = directory(crate::upgrade::AT_REST_LEVEL - 1);
        files.write("SESSION.memory", b"1234\n").unwrap();
        assert_eq!(
            std::fs::read(dir.path().join("SESSION.memory")).unwrap(),
            b"1234\n"
        );
        assert_eq!(
            files.read("SESSION.memory", FOUNDED).unwrap().unwrap(),
            b"1234\n"
        );
        crate::storage_level::record(dir.path(), crate::upgrade::AT_REST_LEVEL).unwrap();
        files.write("SESSION.memory", b"5678\n").unwrap();
        let on_disk = std::fs::read(dir.path().join("SESSION.memory")).unwrap();
        assert!(on_disk.starts_with(SEALED_MAGIC));
        assert!(!on_disk.windows(4).any(|window| window == b"5678"));
        assert_eq!(
            files.read("SESSION.memory", FOUNDED).unwrap().unwrap(),
            b"5678\n"
        );
    }

    /// The founded allowance's bound, for the tests' reads of it.
    const FOUNDED: usize = crate::memory_envelope::FOUNDED_BYTES;

    /// Every node file in the clear is checked and sealed, then the directory is marked; each reads
    /// back as it was, and a second conversion does nothing.
    #[test]
    fn a_conversion_seals_every_file_in_the_clear_and_marks_the_directory() {
        let (dir, files) = directory(crate::upgrade::AT_REST_LEVEL - 1);
        let planted = plant(&files);
        crate::storage_level::record(dir.path(), crate::upgrade::AT_REST_LEVEL).unwrap();
        let converted = files.convert().unwrap();
        assert_eq!(
            converted,
            Converted {
                sealed: planted.len(),
                found_sealed: 0
            }
        );
        assert!(files.marked().unwrap());
        for (name, bytes) in &planted {
            let on_disk = std::fs::read(dir.path().join(name)).unwrap();
            assert!(on_disk.starts_with(SEALED_MAGIC), "{name} not sealed");
            assert_eq!(
                &files.read(name, 1 << 20).unwrap().unwrap(),
                bytes,
                "{name}"
            );
        }
        assert_eq!(files.convert().unwrap(), Converted::default());
    }

    /// A conversion cut after it sealed some files and before its mark, an install's temporary left
    /// holding a file in the clear, is finished by the next: what the cut sealed is found sealed,
    /// the rest sealed, the temporary removed, nothing lost.
    #[test]
    fn a_conversion_cut_before_its_mark_is_finished_by_the_next() {
        let (dir, files) = directory(crate::upgrade::AT_REST_LEVEL - 1);
        let planted = plant(&files);
        crate::storage_level::record(dir.path(), crate::upgrade::AT_REST_LEVEL).unwrap();
        let key = files.journal_key().unwrap();
        let mut cut = Converted::default();
        for (name, kind) in fixed().into_iter().take(2) {
            files.convert_one(&key, name, kind, &mut cut).unwrap();
        }
        assert_eq!(cut.sealed, 2);
        let temporary = dir.path().join("SESSION.install");
        std::fs::write(&temporary, b"1073741824\n").unwrap();
        // And what an earlier binary's cut adoption left beside the adopted issuers.
        let staged = dir
            .path()
            .join("CLIENT.contexts/enrollment-alice/trust-adopted.bin.next");
        std::fs::write(&staged, b"FCLTRST1").unwrap();
        assert!(!files.marked().unwrap());
        let finished = files.convert().unwrap();
        assert_eq!(finished.found_sealed, 2);
        assert_eq!(finished.sealed, planted.len() - 2);
        assert!(
            !temporary.exists(),
            "the cut install's temporary stays in the clear"
        );
        assert!(
            !staged.exists(),
            "the cut adoption's temporary stays in the clear"
        );
        assert!(files.marked().unwrap());
        for (name, bytes) in &planted {
            assert_eq!(
                &files.read(name, 1 << 20).unwrap().unwrap(),
                bytes,
                "{name}"
            );
        }
    }

    /// A conversion cut after any file's sealed write and before its mark is finished by the next:
    /// at every cut point (none of the files sealed, the first, the first two, ..., all of them and
    /// no mark), with an install's temporary and an earlier binary's staged adoption left beside
    /// them in the clear, the next conversion finds what the cut sealed, seals the rest, removes the
    /// temporaries and marks the directory, and every file reads back as it was. A file's
    /// conversion is its own (checked, then written sealed through its atomic replace, as a write
    /// from the level is), so the files before a cut are those written sealed.
    #[test]
    fn a_conversion_cut_after_any_files_sealed_write_is_finished_by_the_next() {
        let mut cut = 0;
        loop {
            let (dir, files) = directory(crate::upgrade::AT_REST_LEVEL - 1);
            let planted = plant(&files);
            if cut > planted.len() {
                break;
            }
            crate::storage_level::record(dir.path(), crate::upgrade::AT_REST_LEVEL).unwrap();
            for (name, bytes) in &planted[..cut] {
                files.write(name, bytes).unwrap();
            }
            let temporary = dir.path().join("SESSION.install");
            std::fs::write(&temporary, b"1073741824\n").unwrap();
            let staged = dir
                .path()
                .join("CLIENT.contexts/enrollment-alice/trust-adopted.bin.next");
            std::fs::write(&staged, b"FCLTRST1").unwrap();
            assert!(!files.marked().unwrap());
            assert_eq!(
                files.convert().unwrap(),
                Converted {
                    sealed: planted.len() - cut,
                    found_sealed: cut
                },
                "cut after {cut} files"
            );
            assert!(!temporary.exists(), "cut after {cut} files");
            assert!(!staged.exists(), "cut after {cut} files");
            assert!(files.marked().unwrap());
            for (name, bytes) in &planted {
                let on_disk = std::fs::read(dir.path().join(name)).unwrap();
                assert!(on_disk.starts_with(SEALED_MAGIC), "{name}, cut after {cut}");
                assert_eq!(
                    &files.read(name, 1 << 20).unwrap().unwrap(),
                    bytes,
                    "{name}, cut after {cut}"
                );
            }
            cut += 1;
        }
    }

    /// From the mark a node file in the clear is refused, as a sealed file moved to another's name,
    /// one carried from another directory, and one with a changed byte are.
    #[test]
    fn a_planted_moved_carried_or_changed_node_file_is_refused() {
        let (dir, files) = directory(crate::upgrade::AT_REST_LEVEL - 1);
        plant(&files);
        crate::storage_level::record(dir.path(), crate::upgrade::AT_REST_LEVEL).unwrap();
        files.convert().unwrap();
        let memory = dir.path().join("SESSION.memory");
        let sealed = std::fs::read(&memory).unwrap();

        std::fs::write(&memory, b"1073741824\n").unwrap();
        assert!(matches!(
            files.read("SESSION.memory", FOUNDED),
            Err(NodeError::ClearAfterSealed(name)) if name == "SESSION.memory"
        ));

        std::fs::copy(dir.path().join("cluster/pending-retires"), &memory).unwrap();
        assert!(matches!(
            files.read("SESSION.memory", 1 << 20),
            Err(NodeError::MovedNodeFile(name)) if name == "SESSION.memory"
        ));

        let (other, other_files) = directory(crate::upgrade::AT_REST_LEVEL);
        other_files.write("SESSION.memory", b"42\n").unwrap();
        std::fs::copy(other.path().join("SESSION.memory"), &memory).unwrap();
        assert!(matches!(
            files.read("SESSION.memory", FOUNDED),
            Err(NodeError::SealedFile(_))
        ));

        let mut changed = sealed.clone();
        let last = changed.len() - 1;
        changed[last] ^= 1;
        std::fs::write(&memory, &changed).unwrap();
        assert!(matches!(
            files.read("SESSION.memory", FOUNDED),
            Err(NodeError::SealedFile(_))
        ));
        std::fs::write(&memory, &sealed).unwrap();
        assert_eq!(
            files.read("SESSION.memory", FOUNDED).unwrap().unwrap(),
            b"1073741824\n"
        );
    }

    /// A directory whose keys an earlier binary made under the file named for its node has that file
    /// named for its root key's ID before its identity is sealed, and its keys are found without its
    /// identity after.
    #[test]
    fn a_legacy_key_file_is_named_for_its_root_key_before_the_identity_is_sealed() {
        let dir = tempfile::tempdir().unwrap();
        let identity = crate::embedded::new_identity().unwrap();
        let legacy = focal_seal::default_key_file(&format!("{:016x}", identity.node)).unwrap();
        let keys = focal_seal::DataKeys::new(dir.path(), &legacy);
        keys.ensure().unwrap();
        let files = NodeFiles::new(dir.path(), Some(keys.clone()));
        crate::storage_level::record(dir.path(), crate::upgrade::AT_REST_LEVEL - 1).unwrap();
        let payload = postcard::to_stdvec(&identity).unwrap();
        let mut bytes = b"FOCALND1".to_vec();
        bytes.extend_from_slice(blake3::hash(&payload).as_bytes());
        bytes.extend_from_slice(&payload);
        files.write("IDENTITY", &bytes).unwrap();
        let mut settings = crate::config::Settings::default();
        settings.node.data_dir = Some(dir.path().to_path_buf());
        assert_eq!(settings.data_keys(dir.path()).unwrap().key_file, legacy);

        settings.name_key_file(dir.path(), &keys).unwrap();
        crate::storage_level::record(dir.path(), crate::upgrade::AT_REST_LEVEL).unwrap();
        files.convert().unwrap();
        assert!(
            std::fs::read(dir.path().join("IDENTITY"))
                .unwrap()
                .starts_with(SEALED_MAGIC)
        );
        let found = settings.data_keys(dir.path()).unwrap();
        let named =
            focal_seal::key_file_for(focal_seal::recorded_root(dir.path()).unwrap().unwrap())
                .unwrap();
        assert_eq!(found.key_file, named);
        let reopened = NodeFiles::new(dir.path(), Some(found));
        assert_eq!(
            reopened
                .read("IDENTITY", crate::embedded::IDENTITY_BYTES)
                .unwrap()
                .unwrap(),
            bytes
        );
        let _ = std::fs::remove_file(&legacy);
        let _ = std::fs::remove_file(&named);
    }

    /// Measures a node file's write sealed against the same bytes installed in the clear as an
    /// earlier binary wrote them, at a founded allowance's size, an identity's, a deployment
    /// journal's and its bound's: the journal key's unwrap, the seal, and the whole write, medians
    /// of 101 rounds; then a whole conversion of a directory holding every kind of node file and a
    /// record for each partition a node may host, the median of 11. Run by hand, in release:
    /// `cargo test -p focal-node --release --lib -- --ignored measure_node_files --nocapture`.
    #[test]
    #[ignore = "a measurement, run by hand"]
    fn measure_node_files() {
        const ROUNDS: usize = 101;
        let median = |mut times: Vec<std::time::Duration>| {
            times.sort_unstable();
            times[times.len() / 2]
        };
        let (dir, files) = directory(crate::upgrade::AT_REST_LEVEL);
        let plain = dir.path().join("PLAIN");
        for size in [21, 256, 64 << 10, 1 << 20] {
            let bytes = vec![0x5a_u8; size];
            let (mut unwrap, mut sealing, mut sealed, mut clear) =
                (Vec::new(), Vec::new(), Vec::new(), Vec::new());
            for _ in 0..ROUNDS {
                let began = std::time::Instant::now();
                let key = files.journal_key().unwrap();
                unwrap.push(began.elapsed());
                let began = std::time::Instant::now();
                std::hint::black_box(seal(&key, "POLICY", &bytes).unwrap());
                sealing.push(began.elapsed());
                drop(key);
                let began = std::time::Instant::now();
                files.write("POLICY", &bytes).unwrap();
                sealed.push(began.elapsed());
                let began = std::time::Instant::now();
                atomic_file(&plain, &bytes).unwrap();
                clear.push(began.elapsed());
            }
            eprintln!(
                "node file of {size} B: key unwrap {:?}, seal {:?}; write sealed {:?}, in the clear {:?}",
                median(unwrap),
                median(sealing),
                median(sealed),
                median(clear)
            );
        }
        let mut conversions = Vec::new();
        for _ in 0..11 {
            let (dir, files) = directory(crate::upgrade::AT_REST_LEVEL - 1);
            let planted = plant(&files);
            let (_, record) = planted
                .iter()
                .find(|(name, _)| name.starts_with("cluster/partitions/"))
                .unwrap();
            for partition in 1..MOST_PARTITION_RECORDS {
                let name = format!("cluster/partitions/{partition:032x}.partition");
                std::fs::write(dir.path().join(name), record).unwrap();
            }
            crate::storage_level::record(dir.path(), crate::upgrade::AT_REST_LEVEL).unwrap();
            let began = std::time::Instant::now();
            let converted = files.convert().unwrap();
            conversions.push((began.elapsed(), converted.sealed));
        }
        conversions.sort_unstable();
        let (took, sealed) = conversions[conversions.len() / 2];
        eprintln!("a conversion of {sealed} node files: {took:?}");
    }
}
