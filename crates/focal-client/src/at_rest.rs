//! A client store's records at rest (29 §12.11): the operation store's catalogue and prepared
//! records, each operation's pending journal, the native, managed and watch stores' records, and an
//! upload's transfer state. Each record is the whole file its store has always written (its magic,
//! its length, its bytes and their BLAKE3 hash); below the at-rest level it is written byte for byte
//! so, where a binary below the level may still be rolled back to, and from it the same bytes are
//! sealed whole under the data directory's journal key (`Store::Journal`, `focal_seal::named`), the
//! record's path relative to the data directory bound inside, so a record moved to another's name or
//! another store, or carried in from another data directory, is refused as a changed byte is. A store
//! its operator put outside the data directory (an operation journal at a path it named) is bound to
//! where it lies, its canonical path whole: moved, it is refused as a moved record is.
//!
//! A store whose data directory's fence opens the level converts its records at its open, under its
//! lock: each record in the clear read and checked as its reader checks it, written sealed through
//! the replace that writes it, then the store marked ([`SEALED`]); from the mark a record in the
//! clear is refused, and what the store writes is sealed whatever form it was asked for. Before the
//! mark a reader takes either form.
//!
//! An upload's staged bytes are a part (focal-evidence `record_file`), sealed as they are written
//! under a key of their own ([`Records::part_key`]), as a node's staging keeps an upload's.
use std::cell::Cell;
use std::path::{Path, PathBuf};

use focal_seal::named::{Kind, NamedError};
use focal_seal::{DataKeys, Store};
use hyper_seal::keys::WrappingKey;
use hyper_seal::stream::VersionKey;
use zeroize::Zeroizing;

/// Format: what a sealed client record begins with, in the clear: its kind, and nothing of what it
/// holds.
pub const SEALED_MAGIC: &[u8; 8] = b"FCLCSEAL";

/// Format: a client record sealed whole: [`SEALED_MAGIC`] in the clear, then the seal of its frame,
/// `FCLCREC1`, the record's path relative to its data directory (`/` between its parts), and the
/// record's bytes as its store writes them in the clear.
const KIND: Kind = Kind {
    outer: SEALED_MAGIC,
    frame: b"FCLCREC1",
};

/// Format: the mark of a client store whose records are all sealed: from it a record in the clear is
/// refused. An empty file, in the clear by design (29 §12).
pub const SEALED: &str = "SEALED";

/// Format: what a part's key is derived for under the journal key (HKDF-SHA-384's purpose), its
/// label the part's file ID and the hash of its bound name.
const PARTS: &[u8] = b"focal client part key";

/// Why a client store's record was not sealed or opened.
#[derive(Debug, thiserror::Error)]
pub enum AtRestError {
    /// The data directory's keys did not open (29 §2).
    #[error("the data directory's keys: {0}")]
    Keys(#[from] focal_seal::SealSetupError),
    /// A sealed record that did not open under the data directory's keys: changed, cut, moved to
    /// another record's name or store, or carried in from another data directory.
    #[error("a sealed client record: {0}")]
    Sealed(#[from] NamedError),
    /// A record in the clear in a store marked sealed: planted or restored over, refused.
    #[error("the client record {0} is in the clear in a store whose records are sealed")]
    ClearAfterSealed(String),
    /// A store asked to seal, or a sealed record read, with no keys for its data directory.
    #[error("a client store sealed without its data directory's keys")]
    WithoutKeys,
    /// A store whose path no seal can bind: one that does not resolve to a path in its directory.
    #[error("the client store {} has no name a seal can bind", .0.display())]
    Unnamed(PathBuf),
    /// A part's key was not derived.
    #[error("a client part's key: {0}")]
    Key(#[from] hyper_seal::SealError),
    #[error("client store I/O: {0}")]
    Io(#[from] std::io::Error),
}

/// How a client store keeps its records at rest: its data directory's keys, and whether what it
/// writes is sealed: the fence its data directory records opens the at-rest level, which its caller
/// reads (29 §12.11: the node's recorded fence where the store lies in a node's data directory, the
/// fence the client learned from its cluster in its own).
#[derive(Clone, Debug, Default)]
pub struct AtRest {
    keys: Option<DataKeys>,
    seal: bool,
}

impl AtRest {
    /// Records written in the clear, byte for byte as every earlier binary wrote them, and none
    /// sealed read: a store whose data directory has no keys.
    pub fn clear() -> Self {
        Self::default()
    }

    /// Records sealed under `keys` where `seal`, written in the clear otherwise and read in either
    /// form until the store is marked.
    pub fn new(keys: Option<DataKeys>, seal: bool) -> Self {
        Self { keys, seal }
    }

    /// Whether what the store writes is sealed.
    pub fn sealing(&self) -> bool {
        self.seal
    }
}

/// A store's records under its [`AtRest`]: what a record's clear bytes are written as, and what a
/// record read holds in the clear.
#[derive(Debug)]
pub(crate) struct Records {
    rest: AtRest,
    /// The store's root, which its mark lies in.
    root: PathBuf,
    /// The mark's name in the root: [`SEALED`], or an owner's own where several share a directory.
    mark: String,
    /// The store's root relative to its data directory, resolved through links (its canonical path
    /// where it lies outside it): the start of every record's bound name. None where the data
    /// directory has no keys.
    prefix: Option<String>,
    /// Whether the store is marked. Looked for once, when its owner's lock is held (every
    /// [`Records`] is made under it, and a mark is made only under it), and set by [`Records::mark`].
    marked: Cell<bool>,
}

impl Records {
    /// The records of the store at `root`, kept as `rest` says. A store asked to seal with no keys
    /// is refused.
    pub(crate) fn new(root: &Path, rest: AtRest) -> Result<Self, AtRestError> {
        Self::owned(root, rest, SEALED.to_owned())
    }

    /// [`Records::new`] for one owner of several sharing the directory `root`, marked by its own
    /// `mark`. A marked store seals what it writes whatever `rest` asks, so it needs its keys.
    pub(crate) fn owned(root: &Path, rest: AtRest, mark: String) -> Result<Self, AtRestError> {
        let marked = root.join(&mark).try_exists()?;
        let prefix = match &rest.keys {
            Some(keys) => Some(prefix(&keys.data_dir, root)?),
            None if rest.seal || marked => return Err(AtRestError::WithoutKeys),
            None => None,
        };
        Ok(Self {
            rest,
            root: root.to_path_buf(),
            mark,
            prefix,
            marked: Cell::new(marked),
        })
    }

    /// Whether what the store writes now is sealed: its form asks it, or it is marked.
    pub(crate) fn sealing(&self) -> bool {
        self.rest.seal || self.marked.get()
    }

    /// Whether the store is marked: every record sealed, and a record in the clear refused.
    pub(crate) fn marked(&self) -> Result<bool, AtRestError> {
        Ok(self.marked.get())
    }

    /// Marks the store: every record sealed. What a conversion does last, and what a store made
    /// sealed does first.
    pub(crate) fn mark(&self) -> Result<(), AtRestError> {
        let path = self.root.join(&self.mark);
        match focal_platform::fs::create_private_new(&path, false, true) {
            Ok(file) => file.sync_all()?,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        focal_platform::sync_dir(&self.root)?;
        self.marked.set(true);
        Ok(())
    }

    /// The most bytes a record holding `most` bytes in the clear takes on disk, in either form.
    pub(crate) fn bound(&self, most: usize) -> Result<usize, AtRestError> {
        Ok(KIND.bound(most)?)
    }

    /// What the record `relative` (a path in the store) holding `clear` is written as: sealed where
    /// the store seals, `clear` itself where it does not.
    pub(crate) fn to_disk(&self, relative: &str, clear: Vec<u8>) -> Result<Vec<u8>, AtRestError> {
        if !self.sealing() {
            return Ok(clear);
        }
        let clear = Zeroizing::new(clear);
        Ok(KIND.seal(&self.key()?, &self.name(relative)?, &clear)?)
    }

    /// The clear bytes of the record `relative`, read from disk as `bytes`, no more than `most` of
    /// them: opened where sealed, taken as they are in the clear until the store's mark.
    pub(crate) fn opened(
        &self,
        relative: &str,
        bytes: Vec<u8>,
        most: usize,
    ) -> Result<Vec<u8>, AtRestError> {
        if KIND.sealed(&bytes) {
            let mut opened = KIND.open(&self.key()?, &self.name(relative)?, bytes, most)?;
            return Ok(std::mem::take(&mut *opened));
        }
        if self.marked.get() {
            return Err(AtRestError::ClearAfterSealed(relative.to_owned()));
        }
        Ok(bytes)
    }

    /// The key of the part `relative` (a record file in the store) whose file ID is `file`:
    /// derived from the journal key, the ID and the hash of the part's bound name together
    /// (HKDF-SHA-384), so every part has a key of its own, and one moved under another part's name
    /// or into another store opens to nothing at its header.
    pub(crate) fn part_key(
        &self,
        relative: &str,
        file: &[u8; 16],
    ) -> Result<VersionKey, AtRestError> {
        let owner = blake3::hash(self.name(relative)?.as_bytes());
        let mut label = [0u8; 48];
        let (id, rest) = label
            .split_at_mut_checked(file.len())
            .ok_or(NamedError::Capacity)?;
        id.copy_from_slice(file);
        rest.copy_from_slice(owner.as_bytes());
        let (secret, _) = self.key()?.derive_child(PARTS, &label)?;
        Ok(VersionKey::new(&secret, *file)?)
    }

    /// Whether the bytes `bytes` read from disk are a sealed record's.
    pub(crate) fn sealed(bytes: &[u8]) -> bool {
        KIND.sealed(bytes)
    }

    /// The key records are sealed under: the data directory's journal key, unwrapped for the write
    /// or read, or the conversion, that takes it.
    fn key(&self) -> Result<WrappingKey, AtRestError> {
        let keys = self.rest.keys.as_ref().ok_or(AtRestError::WithoutKeys)?;
        Ok(keys.key(Store::Journal)?)
    }

    /// The name the record `relative` is bound to: its path relative to its data directory.
    fn name(&self, relative: &str) -> Result<String, AtRestError> {
        let prefix = self.prefix.as_deref().ok_or(AtRestError::WithoutKeys)?;
        let mut name = String::new();
        name.try_reserve_exact(
            prefix
                .len()
                .saturating_add(1)
                .saturating_add(relative.len()),
        )
        .map_err(|_| NamedError::Capacity)?;
        if !prefix.is_empty() {
            name.push_str(prefix);
            name.push('/');
        }
        name.push_str(relative);
        Ok(name)
    }
}

/// A step of a conversion (29 §12.11), each after a durable change: where a test cuts one short to
/// see the next open finish it. Nothing outside tests.
pub(crate) fn step() -> Result<(), AtRestError> {
    #[cfg(test)]
    if cut::taken() {
        return Err(AtRestError::Io(std::io::Error::other(
            "a conversion cut short",
        )));
    }
    Ok(())
}

/// Cutting a conversion short at a step, for tests.
#[cfg(test)]
pub(crate) mod cut {
    use std::cell::Cell;

    thread_local! {
        static LEFT: Cell<Option<usize>> = const { Cell::new(None) };
    }

    /// The next conversion on this thread cut after `steps` more steps.
    pub(crate) fn after(steps: usize) {
        LEFT.with(|left| left.set(Some(steps)));
    }

    /// No conversion on this thread cut.
    pub(crate) fn none() {
        LEFT.with(|left| left.set(None));
    }

    /// Whether the conversion is cut here.
    pub(super) fn taken() -> bool {
        LEFT.with(|left| match left.get() {
            Some(0) => {
                left.set(None);
                true
            }
            Some(steps) => {
                left.set(Some(steps.saturating_sub(1)));
                false
            }
            None => false,
        })
    }
}

/// A file a store's owner keeps beside its store, kept at rest as the store's records are (29
/// §12.11): a bootstrap marker, which names the context its store was made for. Below the level its
/// bytes are written as every earlier binary wrote them; from it they are sealed whole, its name
/// bound inside; at its owner's next open under the owner's lock one in the clear is written again
/// sealed through a replace, and the file marked (`<name>-sealed`), from which one in the clear is
/// refused. Its owner opens and checks the file itself, as it opens its others.
pub struct OwnerFile {
    records: Records,
    parent: PathBuf,
    name: String,
}

impl OwnerFile {
    /// The file `name` in its owner's directory `parent`, kept as `rest` says. Made under the
    /// owner's lock.
    pub fn new(parent: &Path, name: &str, rest: AtRest) -> Result<Self, AtRestError> {
        Ok(Self {
            records: Records::owned(parent, rest, format!("{name}-sealed"))?,
            parent: parent.to_path_buf(),
            name: name.to_owned(),
        })
    }

    /// Whether what it writes is sealed.
    pub fn sealing(&self) -> bool {
        self.records.sealing()
    }

    /// Marks it sealed: what an owner that makes the file sealed does first.
    pub fn mark(&self) -> Result<(), AtRestError> {
        self.records.mark()
    }

    /// The most bytes the file holding `most` bytes takes on disk, in either form.
    pub fn bound(&self, most: usize) -> Result<usize, AtRestError> {
        self.records.bound(most)
    }

    /// What its bytes `clear` are written as: sealed where it seals, `clear` itself where not.
    pub fn to_disk(&self, clear: Vec<u8>) -> Result<Vec<u8>, AtRestError> {
        self.records.to_disk(&self.name, clear)
    }

    /// Its bytes, read from disk as `bytes`, no more than `most` of them: opened where sealed, in
    /// the clear until it is marked.
    pub fn opened(&self, bytes: Vec<u8>, most: usize) -> Result<Vec<u8>, AtRestError> {
        self.records.opened(&self.name, bytes, most)
    }

    /// Whether `bytes` read from disk are sealed.
    pub fn sealed(bytes: &[u8]) -> bool {
        Records::sealed(bytes)
    }

    /// Converts it where it seals and is not marked, its bytes `clear` as read from `on_disk`:
    /// written again sealed where they lie in the clear, through its own temporary
    /// (`<name>.pending`, any a cut left removed first), synced, renamed over it, the directory
    /// synced; then marked.
    pub fn convert(&self, on_disk: &[u8], clear: &[u8]) -> Result<(), AtRestError> {
        if !self.records.sealing() || self.records.marked()? {
            return Ok(());
        }
        if !Records::sealed(on_disk) {
            let temporary = self.parent.join(format!("{}.pending", self.name));
            match std::fs::remove_file(&temporary) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(clear.len())
                .map_err(|_| NamedError::Capacity)?;
            bytes.extend_from_slice(clear);
            let sealed = self.records.to_disk(&self.name, bytes)?;
            let mut file = focal_platform::fs::create_private_new(&temporary, false, true)?;
            std::io::Write::write_all(&mut file, &sealed)?;
            file.sync_all()?;
            // Closed before the rename: Windows renames no open file.
            drop(file);
            focal_platform::fs::atomic_replace(&temporary, &self.parent.join(&self.name))?;
            focal_platform::sync_dir(&self.parent)?;
            step()?;
        }
        self.records.mark()
    }
}

/// `root` relative to the data directory `data`, each resolved through its links (a command line
/// names its data directory as it was given, a store by the directory it canonicalized: macOS's
/// temporary directories are `/var` and `/private/var` of one directory), `/` between its parts. A
/// root outside `data` is its canonical path whole, which no relative name begins as (one begins
/// with its root or its drive). A path that is not UTF-8 is named lossily and told apart from
/// another named so by the BLAKE3 hash of its bytes after a `#`.
fn prefix(data: &Path, root: &Path) -> Result<String, AtRestError> {
    let data = data.canonicalize()?;
    let root = root.canonicalize()?;
    let (subject, mut prefix) = match root.strip_prefix(&data) {
        Ok(relative) => {
            let mut prefix = String::new();
            for part in relative.components() {
                let std::path::Component::Normal(part) = part else {
                    return Err(AtRestError::Unnamed(root.clone()));
                };
                if !prefix.is_empty() {
                    prefix.push('/');
                }
                prefix.push_str(&part.to_string_lossy());
            }
            (relative, prefix)
        }
        Err(_) => (root.as_path(), root.to_string_lossy().into_owned()),
    };
    if subject.to_str().is_none() {
        prefix.push('#');
        prefix.push_str(blake3::hash(&os_bytes(subject)).to_hex().as_str());
    }
    Ok(prefix)
}

/// The bytes of `path` as its platform holds them.
fn os_bytes(path: &Path) -> Vec<u8> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        path.as_os_str().as_bytes().to_vec()
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        path.as_os_str()
            .encode_wide()
            .flat_map(u16::to_le_bytes)
            .collect()
    }
    #[cfg(not(any(unix, windows)))]
    {
        path.to_string_lossy().as_bytes().to_vec()
    }
}

/// What the stores' at-rest tests share: a data directory with keys, the two forms, and the check
/// that nothing of a store lies in the clear.
#[cfg(test)]
pub(crate) mod support {
    use super::*;

    /// A data directory with keys; a store's root lies inside it.
    pub(crate) fn directory() -> (tempfile::TempDir, DataKeys) {
        let directory = tempfile::tempdir().unwrap();
        let keys = focal_seal::test_keys(directory.path()).unwrap();
        (directory, keys)
    }

    /// Below the level: the keys are there, nothing is sealed.
    pub(crate) fn below(keys: &DataKeys) -> AtRest {
        AtRest::new(Some(keys.clone()), false)
    }

    /// From the level.
    pub(crate) fn sealing(keys: &DataKeys) -> AtRest {
        AtRest::new(Some(keys.clone()), true)
    }

    /// Whether a file of this name is one a store keeps in the clear by design: a lock, a marker of
    /// its making (a fixed sentence), or a mark.
    fn by_design(name: &str) -> bool {
        name == "LOCK"
            || name == "INITIALIZED"
            || name == SEALED
            || name.ends_with("-sealed")
            || name.ends_with("-lock")
            || name.ends_with(".lock")
    }

    /// Every file under `root` sealed (a record sealed whole, or a part) but the locks, markers and
    /// marks a store keeps in the clear by design, which hold a fixed sentence at most; and none
    /// holds any of `needles`.
    pub(crate) fn assert_sealed_under(root: &Path, needles: &[&[u8]]) {
        let mut pending = vec![root.to_path_buf()];
        let mut files = 0;
        while let Some(directory) = pending.pop() {
            for entry in std::fs::read_dir(&directory).unwrap() {
                let entry = entry.unwrap();
                let path = entry.path();
                if entry.file_type().unwrap().is_dir() {
                    pending.push(path);
                    continue;
                }
                let name = entry.file_name().to_string_lossy().into_owned();
                if by_design(&name) {
                    // Its length alone: a lock an open store holds is not read on Windows.
                    assert!(
                        std::fs::metadata(&path).unwrap().len() <= 8,
                        "{} holds more than a marker",
                        path.display()
                    );
                    continue;
                }
                let bytes = std::fs::read(&path).unwrap();
                files += 1;
                assert!(
                    bytes.starts_with(SEALED_MAGIC)
                        || bytes.starts_with(focal_evidence::record_file::PART_MAGIC),
                    "{} lies in the clear",
                    path.display()
                );
                for needle in needles {
                    assert!(
                        !bytes.windows(needle.len()).any(|window| window == *needle),
                        "{} holds {:?} in the clear",
                        path.display(),
                        String::from_utf8_lossy(needle)
                    );
                }
            }
        }
        assert!(files > 0, "no record under {}", root.display());
    }

    /// The file at `path` with its last byte changed.
    pub(crate) fn flip(path: &Path) {
        let mut bytes = std::fs::read(path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        std::fs::write(path, bytes).unwrap();
    }

    /// The files at `first` and `second` swapped.
    pub(crate) fn swap(first: &Path, second: &Path) {
        let one = std::fs::read(first).unwrap();
        let other = std::fs::read(second).unwrap();
        std::fs::write(first, other).unwrap();
        std::fs::write(second, one).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A data directory with keys and a store's root inside it.
    fn store() -> (tempfile::TempDir, PathBuf, DataKeys) {
        let dir = tempfile::tempdir().unwrap();
        let keys = focal_seal::test_keys(dir.path()).unwrap();
        let root = dir.path().join("client/native");
        std::fs::create_dir_all(&root).unwrap();
        (dir, root, keys)
    }

    /// Below the level a record is written as its store's clear bytes, byte for byte; from it the
    /// same bytes sealed, nowhere in the clear, read back whole; a clear record taken until the
    /// store's mark and refused after it.
    #[test]
    fn a_record_is_written_as_today_below_the_level_and_sealed_from_it() {
        let (_dir, root, keys) = store();
        let clear = b"FCLID001\x00\x00\x00\x05bytes the store's own".to_vec();
        let below = Records::new(&root, AtRest::new(Some(keys.clone()), false)).unwrap();
        assert_eq!(
            below.to_disk("catalogue.bin", clear.clone()).unwrap(),
            clear
        );
        let at = Records::new(&root, AtRest::new(Some(keys.clone()), true)).unwrap();
        let sealed = at.to_disk("catalogue.bin", clear.clone()).unwrap();
        assert!(Records::sealed(&sealed));
        assert!(!sealed.windows(8).any(|window| window == b"FCLID001"));
        assert_eq!(
            at.opened("catalogue.bin", sealed.clone(), clear.len())
                .unwrap(),
            clear
        );
        assert_eq!(
            at.opened("catalogue.bin", clear.clone(), clear.len())
                .unwrap(),
            clear
        );
        at.mark().unwrap();
        assert!(matches!(
            at.opened("catalogue.bin", clear.clone(), clear.len()),
            Err(AtRestError::ClearAfterSealed(name)) if name == "catalogue.bin"
        ));
        assert!(matches!(
            at.opened("prepared.bin", sealed, clear.len()),
            Err(AtRestError::Sealed(NamedError::Moved(_)))
        ));
    }

    /// What sealing a client record costs (29 §12.11): sealed and opened whole at a record's sizes,
    /// the journal key unwrapped for each as a write unwraps it; the median of 101 rounds. Run by
    /// hand, in release: `cargo test -p focal-client --release --lib -- --ignored
    /// measure_record_seal --nocapture`.
    #[test]
    #[ignore = "a measurement, run by hand"]
    fn measure_record_seal() {
        const ROUNDS: usize = 101;
        let median = |mut times: Vec<std::time::Duration>| {
            times.sort_unstable();
            times[times.len() / 2]
        };
        let (_dir, root, keys) = store();
        let records = Records::new(&root, AtRest::new(Some(keys), true)).unwrap();
        for size in [256, 4 << 10, 64 << 10, 1 << 20] {
            let bytes = vec![0x5a_u8; size];
            let (mut sealing, mut opening) = (Vec::new(), Vec::new());
            for _ in 0..ROUNDS {
                let began = std::time::Instant::now();
                let sealed = records.to_disk("state.bin", bytes.clone()).unwrap();
                sealing.push(began.elapsed());
                let began = std::time::Instant::now();
                std::hint::black_box(records.opened("state.bin", sealed, size).unwrap());
                opening.push(began.elapsed());
            }
            eprintln!(
                "a client record of {size} B: sealed {:?}, opened {:?}",
                median(sealing),
                median(opening)
            );
        }
    }

    /// A record moved to another store under the same name, and one sealed in another data
    /// directory, are refused; a store asked to seal without keys is refused at its open; a store
    /// outside its data directory is bound to where it lies, and refused moved.
    #[test]
    fn a_record_of_another_store_or_directory_is_refused() {
        let (dir, root, keys) = store();
        let other_root = dir.path().join("MCP.operations");
        std::fs::create_dir_all(&other_root).unwrap();
        let clear = b"FCLID001 a record".to_vec();
        let here = Records::new(&root, AtRest::new(Some(keys.clone()), true)).unwrap();
        let there = Records::new(&other_root, AtRest::new(Some(keys.clone()), true)).unwrap();
        let sealed = there.to_disk("catalogue.bin", clear.clone()).unwrap();
        assert!(matches!(
            here.opened("catalogue.bin", sealed, clear.len()),
            Err(AtRestError::Sealed(NamedError::Moved(_)))
        ));
        let (_far, far_root, far_keys) = store();
        let far = Records::new(&far_root, AtRest::new(Some(far_keys), true)).unwrap();
        let carried = far.to_disk("catalogue.bin", clear.clone()).unwrap();
        assert!(matches!(
            here.opened("catalogue.bin", carried, clear.len()),
            Err(AtRestError::Sealed(NamedError::Seal(_)))
        ));
        assert!(matches!(
            Records::new(&root, AtRest::new(None, true)),
            Err(AtRestError::WithoutKeys)
        ));
        let outside = tempfile::tempdir().unwrap();
        let there = outside.path().join("operation");
        std::fs::create_dir_all(&there).unwrap();
        let away = Records::new(&there, AtRest::new(Some(keys.clone()), true)).unwrap();
        let sealed = away.to_disk("state.bin", clear.clone()).unwrap();
        assert_eq!(
            away.opened("state.bin", sealed.clone(), clear.len())
                .unwrap(),
            clear
        );
        let moved = outside.path().join("moved");
        std::fs::create_dir_all(&moved).unwrap();
        let elsewhere = Records::new(&moved, AtRest::new(Some(keys), true)).unwrap();
        assert!(matches!(
            elsewhere.opened("state.bin", sealed, clear.len()),
            Err(AtRestError::Sealed(NamedError::Moved(_)))
        ));
    }
}
