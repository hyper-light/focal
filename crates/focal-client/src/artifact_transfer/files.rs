use super::{MAX_STATE_BYTES, TRANSFER_CHUNK_BYTES, TransferError};
use crate::at_rest::{AtRest, AtRestError, OwnerFile, Records};
use crate::file_lock::FileLock;
use focal_evidence::ContentError;
use focal_evidence::record_file::{PART_MAGIC, RecordFiles, RecordWriter, Verify, records_bytes};
use hyper_seal::stream::VersionKey;
use std::{
    fs::{self, File},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

#[cfg(all(test, unix))]
#[path = "lock_tests.rs"]
mod lock_tests;

const MARKER: &str = "INITIALIZED";
const PAYLOAD: &str = "payload.bin";
/// Format: an upload's staged bytes from the at-rest level (29 §12.11): a part (focal-evidence
/// `record_file`, `FCLPART1`), sealed as it is written under a key of its own.
const PART: &str = "payload.part";
/// What a part written again (its last append cut short, or cut to its staged bytes) is written
/// into before it is renamed over the part: `record_file`'s `.resume` beside it.
const PART_RESUME: &str = "payload.resume";
const TEMPORARY: &str = ".upload.pending";
#[derive(Clone, Copy)]
enum Layout {
    Upload,
    Catalogue,
}
impl Layout {
    fn magic(self) -> &'static [u8; 8] {
        match self {
            Self::Upload => b"FCLUP001",
            Self::Catalogue => b"FCLUPCAT",
        }
    }
    fn record(self) -> &'static str {
        match self {
            Self::Upload => "upload.bin",
            Self::Catalogue => "uploads.bin",
        }
    }
    fn maximum(self) -> usize {
        match self {
            Self::Upload => MAX_STATE_BYTES,
            Self::Catalogue => super::store::CATALOGUE_BYTES,
        }
    }
}

pub(super) struct Directory {
    path: PathBuf,
    _lock: FileLock,
    layout: Layout,
    /// Its record at rest (29 §12.11): sealed from the at-rest level, in the clear below it.
    records: Records,
    #[cfg(test)]
    fault: std::cell::Cell<Option<Fault>>,
}
impl Directory {
    pub(super) fn path(&self) -> &Path {
        &self.path
    }
    /// A new upload of at most `most` bytes at `path`, kept as `rest` says: its staged bytes a part
    /// sealed as it is written where it seals, a file in the clear below the level.
    pub(super) fn create(
        path: &Path,
        rest: AtRest,
        most: u64,
    ) -> Result<(Self, Payload), TransferError> {
        let directory = Self::create_layout(path, Layout::Upload, rest)?;
        let payload = if directory.records.sealing() {
            Payload::Sealed(Box::new(directory.new_part(most)?))
        } else {
            let payload = focal_platform::fs::create_private_new(&path.join(PAYLOAD), true, true)?;
            directory.check_open(&path.join(PAYLOAD), &payload)?;
            payload.sync_all()?;
            Payload::Clear(payload)
        };
        sync_dir(path)?;
        Ok((directory, payload))
    }
    pub(super) fn create_catalogue(path: &Path, rest: AtRest) -> Result<Self, TransferError> {
        Self::create_layout(path, Layout::Catalogue, rest)
    }
    /// The catalogue at `path`, its record converted to sealed where `rest` seals and it is not
    /// marked yet (29 §12.11): each upload converts at its own open, under its own lock.
    pub(super) fn open_catalogue(path: &Path, rest: AtRest) -> Result<Self, TransferError> {
        let directory = Self::lock(path, false, Layout::Catalogue, rest)?;
        directory.read()?;
        directory.recover_marker()?;
        if directory.records.sealing() && !directory.records.marked()? {
            if !directory.record_sealed()? {
                let record = directory.read()?;
                directory.install(&record, false)?;
                crate::at_rest::step()?;
            }
            directory.records.mark()?;
        }
        Ok(directory)
    }
    fn create_layout(path: &Path, layout: Layout, rest: AtRest) -> Result<Self, TransferError> {
        focal_platform::fs::create_dir_private(path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                TransferError::Exists
            } else {
                e.into()
            }
        })?;
        sync_dir(parent(path))?;
        Self::lock(path, true, layout, rest)
    }
    /// The upload at `path`, its records kept as `rest` says; its staged bytes are opened once it
    /// is converted ([`Directory::convert`], [`Directory::payload`]).
    pub(super) fn open(path: &Path, rest: AtRest) -> Result<Self, TransferError> {
        Self::lock(path, false, Layout::Upload, rest)
    }
    fn lock(
        path: &Path,
        create: bool,
        layout: Layout,
        rest: AtRest,
    ) -> Result<Self, TransferError> {
        let owner = focal_platform::fs::private_dir_owner(path)
            .map_err(missing)?
            .ok_or(TransferError::Permissions)?;
        let lock_path = path.join("LOCK");
        if !create {
            check_file(&lock_path, &owner)?;
        }
        let lock = if create {
            focal_platform::fs::create_private_new(&lock_path, true, true)
        } else {
            focal_platform::fs::open_private(&lock_path, true, true, false)
        }
        .map_err(missing)?;
        check_open_file(&lock_path, &lock, &owner)?;
        let lock = FileLock::acquire(lock).map_err(|e| {
            if e.kind() == std::io::ErrorKind::WouldBlock {
                TransferError::Locked
            } else {
                e.into()
            }
        })?;
        if create {
            lock.file().sync_all()?;
            sync_dir(path)?;
        }
        let records = Records::new(path, rest)?;
        if create && records.sealing() {
            // Made sealed, with nothing in it to convert: a record in the clear is refused from
            // its first.
            records.mark()?;
        }
        Ok(Self {
            path: path.into(),
            _lock: lock,
            layout,
            records,
            #[cfg(test)]
            fault: std::cell::Cell::new(None),
        })
    }
    /// Whether what the upload writes is sealed (29 §12.11).
    pub(super) fn sealing(&self) -> bool {
        self.records.sealing()
    }
    /// Whether its record is sealed on disk: what its first bytes say.
    fn record_sealed(&self) -> Result<bool, TransferError> {
        let mut head = Vec::new();
        self.open_file(self.layout.record(), false)?
            .take(8)
            .read_to_end(&mut head)?;
        Ok(Records::sealed(&head))
    }
    /// Converts the upload to sealed and marks it (29 §12.11), where it seals and is not marked:
    /// its payload in the clear copied into a new part a chunk at a time, `staged` bytes of it (what
    /// its state vouches for: bytes past them were never sent, and go), hashed as they are copied
    /// and checked against `prefix` before anything in the clear goes; the part made durable; its
    /// state sealed through the replace that writes it; the payload in the clear removed; the upload
    /// marked. A cut anywhere leaves the payload in the clear standing until its part is whole (a
    /// part beside a payload in the clear is a cut conversion's, and is made again), and the next
    /// open finishes. `most` is the upload's declared length.
    pub(super) fn convert(
        &self,
        most: u64,
        staged: u64,
        prefix: &[u8; 32],
    ) -> Result<(), TransferError> {
        if !self.records.sealing() || self.records.marked()? {
            return Ok(());
        }
        if present(&self.path.join(PAYLOAD))? {
            let mut clear = self.open_file(PAYLOAD, false)?;
            let held = clear.metadata()?.len();
            if held < staged || held > most {
                return Err(TransferError::Corrupt);
            }
            let mut part = self.fresh_part(most)?;
            let mut scratch = super::buffer(TRANSFER_CHUNK_BYTES)?;
            let mut hash = blake3::Hasher::new();
            let mut copied = 0u64;
            // A chunk a step, each at least a byte: no more than the staged bytes.
            for _ in 0..=staged {
                let remaining = staged.checked_sub(copied).ok_or(TransferError::Corrupt)?;
                if remaining == 0 {
                    break;
                }
                let count = usize::try_from(remaining.min(TRANSFER_CHUNK_BYTES as u64))
                    .map_err(|_| TransferError::Capacity)?;
                let chunk = scratch.get_mut(..count).ok_or(TransferError::Capacity)?;
                clear.read_exact(chunk)?;
                hash.update(chunk);
                part.append(chunk).map_err(part_error)?;
                copied = copied
                    .checked_add(count as u64)
                    .ok_or(TransferError::Capacity)?;
            }
            if copied != staged || hash.finalize().as_bytes() != prefix {
                return Err(TransferError::Corrupt);
            }
            part.sync().map_err(part_error)?;
            // Closed before the payload goes: Windows removes no open file.
            drop(part);
            drop(clear);
            sync_dir(&self.path)?;
            crate::at_rest::step()?;
            if !self.record_sealed()? {
                let state = self.read()?;
                self.install(&state, false)?;
                crate::at_rest::step()?;
            }
            fs::remove_file(self.path.join(PAYLOAD))?;
            sync_dir(&self.path)?;
            crate::at_rest::step()?;
        } else if !self.record_sealed()? {
            let state = self.read()?;
            self.install(&state, false)?;
            crate::at_rest::step()?;
        }
        self.records.mark()?;
        Ok(())
    }
    /// The upload's staged bytes in their form, `most` the upload's declared length: in the clear
    /// where its payload in the clear stands (refused once the upload is marked), else its part,
    /// read through to its end (one whose last append a crash cut short written again whole under a
    /// new key, `record_file`'s resume).
    pub(super) fn payload(&self, most: u64) -> Result<Payload, TransferError> {
        if present(&self.path.join(PAYLOAD))? {
            if self.records.marked()? {
                return Err(AtRestError::ClearAfterSealed(PAYLOAD.to_owned()).into());
            }
            return Ok(Payload::Clear(self.open_file(PAYLOAD, true)?));
        }
        let writer = RecordWriter::resume_in(
            self.private()?,
            &self.path.join(PART),
            PART_MAGIC,
            &|file: &[u8; 16]| self.part_key(file),
            usize::try_from(most).map_err(|_| TransferError::Capacity)?,
            records_bytes(TRANSFER_CHUNK_BYTES).map_err(part_error)?,
            Verify::Lengths,
        )
        .map_err(part_error)?;
        Ok(Payload::Sealed(Box::new(writer)))
    }
    /// A new part of at most `most` bytes, under a key of its own, made durable.
    fn new_part(&self, most: u64) -> Result<RecordWriter<Private>, TransferError> {
        let file = hyper_seal::keys::KeyId::random()
            .map_err(|_| std::io::Error::other("the operating system's random source failed"))?
            .0;
        let key = self.records.part_key(PART, &file)?;
        let writer = RecordWriter::create_in(
            self.private()?,
            &self.path.join(PART),
            PART_MAGIC,
            file,
            key,
            usize::try_from(most).map_err(|_| TransferError::Capacity)?,
        )
        .map_err(part_error)?;
        writer.sync().map_err(part_error)?;
        Ok(writer)
    }
    /// [`Directory::new_part`], once what a cut conversion or a cut rewrite left of a part is gone.
    fn fresh_part(&self, most: u64) -> Result<RecordWriter<Private>, TransferError> {
        for leftover in [PART, PART_RESUME] {
            let path = self.path.join(leftover);
            if present(&path)? {
                self.check_path(&path)?;
                fs::remove_file(&path)?;
            }
        }
        self.new_part(most)
    }
    /// The key of the part's file `file`, for `record_file`.
    fn part_key(&self, file: &[u8; 16]) -> Result<VersionKey, ContentError> {
        self.records
            .part_key(PART, file)
            .map_err(|error| ContentError::Io(std::io::Error::other(error)))
    }
    /// How the part's files are made and opened: as every file of the upload is.
    fn private(&self) -> Result<Private, TransferError> {
        Ok(Private {
            owner: store_owner(&self.path)?,
        })
    }
    fn open_file(&self, name: &str, write: bool) -> Result<File, TransferError> {
        let path = self.path.join(name);
        self.check_path(&path)?;
        let file = focal_platform::fs::open_private(&path, true, write, false).map_err(missing)?;
        self.check_open(&path, &file)?;
        Ok(file)
    }
    fn check_path(&self, path: &Path) -> Result<(), TransferError> {
        check_file(path, &store_owner(&self.path)?)
    }
    fn check_open(&self, path: &Path, file: &File) -> Result<(), TransferError> {
        check_open_file(path, file, &store_owner(&self.path)?)
    }
    pub(super) fn read(&self) -> Result<Vec<u8>, TransferError> {
        let mut file = self.open_file(self.layout.record(), false)?;
        let length =
            usize::try_from(file.metadata()?.len()).map_err(|_| TransferError::Capacity)?;
        let maximum = self
            .layout
            .maximum()
            .checked_add(44)
            .ok_or(TransferError::Capacity)?;
        // Either form: the record's own bytes, or their seal (29 §12.11).
        let on_disk = self.records.bound(maximum)?;
        if !(44..=on_disk).contains(&length) {
            return Err(TransferError::Corrupt);
        }
        let mut bytes = super::buffer(length)?;
        file.read_exact(&mut bytes)?;
        let mut extra = [0; 1];
        if file.read(&mut extra)? != 0 {
            return Err(TransferError::Corrupt);
        }
        let mut bytes = self.records.opened(self.layout.record(), bytes, maximum)?;
        if !(44..=maximum).contains(&bytes.len())
            || bytes.get(..8) != Some(self.layout.magic().as_slice())
        {
            return Err(TransferError::Corrupt);
        }
        let size = u32::from_be_bytes(
            bytes
                .get(8..12)
                .ok_or(TransferError::Corrupt)?
                .try_into()
                .map_err(|_| TransferError::Corrupt)?,
        ) as usize;
        let end = size.checked_add(12).ok_or(TransferError::Corrupt)?;
        if end.checked_add(32) != Some(bytes.len())
            || bytes.get(end..)
                != Some(
                    blake3::hash(bytes.get(..end).ok_or(TransferError::Corrupt)?)
                        .as_bytes()
                        .as_slice(),
                )
        {
            return Err(TransferError::Corrupt);
        }
        bytes.copy_within(12..end, 0);
        bytes.truncate(size);
        Ok(bytes)
    }
    pub(super) fn install(&self, payload: &[u8], initial: bool) -> Result<(), TransferError> {
        if payload.len() > self.layout.maximum() {
            return Err(TransferError::Capacity);
        }
        let state = self.path.join(self.layout.record());
        if initial {
            if present(&state)? || present(&self.path.join(MARKER))? {
                return Err(TransferError::Corrupt);
            }
        } else {
            self.read()?;
            self.check_marker()?;
        }
        let temporary = self.path.join(TEMPORARY);
        if present(&temporary)? {
            self.check_path(&temporary)?;
            fs::remove_file(&temporary)?;
        }
        let length = u32::try_from(payload.len())
            .map_err(|_| TransferError::Capacity)?
            .to_be_bytes();
        let mut hash = blake3::Hasher::new();
        hash.update(self.layout.magic());
        hash.update(&length);
        hash.update(payload);
        // The record whole, as every earlier binary wrote it, then in the form the upload keeps.
        let mut record = Vec::new();
        record
            .try_reserve_exact(payload.len().saturating_add(44))
            .map_err(|_| TransferError::Capacity)?;
        record.extend_from_slice(self.layout.magic());
        record.extend_from_slice(&length);
        record.extend_from_slice(payload);
        record.extend_from_slice(hash.finalize().as_bytes());
        let record = self.records.to_disk(self.layout.record(), record)?;
        let mut file = focal_platform::fs::create_private_new(&temporary, false, true)?;
        file.write_all(&record)?;
        file.sync_all()?;
        // Close before rename: Windows refuses to rename a file with an open handle.
        drop(file);
        #[cfg(test)]
        self.fail_at(Fault::FileSynced)?;
        // The private exclusive lock protects both initial and replacement
        // publication; rename avoids a two-link crash window entirely.
        focal_platform::fs::atomic_replace(&temporary, &state)?;
        #[cfg(test)]
        self.fail_at(Fault::Renamed)?;
        sync_dir(&self.path)?;
        self.recover_marker()
    }
    fn check_marker(&self) -> Result<(), TransferError> {
        let mut file = self.open_file(MARKER, false)?;
        if file.metadata()?.len() != 8 {
            return Err(TransferError::Corrupt);
        }
        let mut value = [0; 8];
        file.read_exact(&mut value)?;
        if &value != self.layout.magic() {
            return Err(TransferError::Corrupt);
        }
        Ok(())
    }
    pub(super) fn recover_marker(&self) -> Result<(), TransferError> {
        // read() validates the record, which is already durable (write() fsyncs
        // it and publishes write-through before the marker is created). A read
        // handle needs no re-sync, and Windows rejects fsync of one.
        self.read()?;
        let marker = self.path.join(MARKER);
        if present(&marker)? {
            return self.check_marker();
        }
        let mut file = focal_platform::fs::create_private_new(&marker, false, true)?;
        file.write_all(self.layout.magic())?;
        file.sync_all()?;
        sync_dir(&self.path)?;
        Ok(())
    }
    #[cfg(test)]
    pub(super) fn inject(&self, fault: Fault) {
        self.fault.set(Some(fault));
    }
    #[cfg(test)]
    fn fail_at(&self, fault: Fault) -> Result<(), TransferError> {
        if self.fault.get() == Some(fault) {
            self.fault.set(None);
            return Err(std::io::Error::other("injected transfer crash").into());
        }
        Ok(())
    }
}
/// An upload's staged bytes: a file in the clear, or a part sealed as it is written (29 §12.11).
pub(super) enum Payload {
    Clear(File),
    Sealed(Box<RecordWriter<Private>>),
}
impl Payload {
    /// The bytes it holds.
    pub(super) fn held(&self) -> Result<u64, TransferError> {
        match self {
            Self::Clear(file) => Ok(file.metadata()?.len()),
            Self::Sealed(writer) => Ok(writer.plain()),
        }
    }
    /// The staged bytes from `offset`, `out.len()` of them: read in place in the clear, opened from
    /// the records that hold them where sealed.
    pub(super) fn read_at(
        &mut self,
        directory: &Directory,
        offset: u64,
        out: &mut [u8],
    ) -> Result<(), TransferError> {
        match self {
            Self::Clear(file) => {
                file.seek(SeekFrom::Start(offset))?;
                file.read_exact(out)?;
                Ok(())
            }
            Self::Sealed(writer) => writer
                .read(
                    &directory.path.join(PART),
                    u32::try_from(offset).map_err(|_| TransferError::Capacity)?,
                    out,
                )
                .map_err(part_error),
        }
    }
    /// `bytes` staged at `offset`, the end of what it holds, and made durable: written in place in
    /// the clear, appended as a record sealed whole where sealed.
    pub(super) fn append(&mut self, offset: u64, bytes: &[u8]) -> Result<(), TransferError> {
        match self {
            Self::Clear(file) => {
                file.seek(SeekFrom::Start(offset))?;
                file.write_all(bytes)?;
                file.sync_all()?;
                Ok(())
            }
            Self::Sealed(writer) => {
                if writer.plain() != offset {
                    return Err(TransferError::Corrupt);
                }
                writer.append(bytes).map_err(part_error)?;
                writer.sync().map_err(part_error)
            }
        }
    }
    /// The payload holding its first `staged` bytes alone: what a crash between a stage's bytes and
    /// its record left past them goes (cut in the clear; sealed, the part's records up to them
    /// written again under a new key, so no place of those let go is sealed over under the old).
    pub(super) fn cut(self, directory: &Directory, staged: u64) -> Result<Self, TransferError> {
        match self {
            Self::Clear(file) => {
                file.set_len(staged)?;
                file.sync_all()?;
                Ok(Self::Clear(file))
            }
            Self::Sealed(writer) => Ok(Self::Sealed(Box::new(
                writer
                    .cut(&directory.path.join(PART), staged, &|file: &[u8; 16]| {
                        directory.part_key(file)
                    })
                    .map_err(part_error)?,
            ))),
        }
    }
    /// The hash of its first `staged` bytes, which must be all it holds: read through in the
    /// clear (its end checked), opened a chunk at a time where sealed.
    pub(super) fn prefix(
        &mut self,
        directory: &Directory,
        staged: u64,
    ) -> Result<blake3::Hasher, TransferError> {
        if let Self::Clear(file) = self {
            return super::hash_reader(file, staged);
        }
        if self.held()? != staged {
            return Err(TransferError::Corrupt);
        }
        let mut scratch = super::buffer(TRANSFER_CHUNK_BYTES)?;
        let mut hash = blake3::Hasher::new();
        let mut read = 0u64;
        // A chunk a step, each at least a byte: no more than the staged bytes.
        for _ in 0..=staged {
            let remaining = staged.checked_sub(read).ok_or(TransferError::Corrupt)?;
            if remaining == 0 {
                return Ok(hash);
            }
            let count = usize::try_from(remaining.min(TRANSFER_CHUNK_BYTES as u64))
                .map_err(|_| TransferError::Capacity)?;
            let chunk = scratch.get_mut(..count).ok_or(TransferError::Capacity)?;
            self.read_at(directory, read, chunk)?;
            hash.update(chunk);
            read = read
                .checked_add(count as u64)
                .ok_or(TransferError::Capacity)?;
        }
        Err(TransferError::Corrupt)
    }
}

/// A part's files made and opened as every file of an upload is: owner-private, each checked
/// against the upload's owner as it is opened (`record_file` takes its files from it).
#[derive(Clone)]
pub(super) struct Private {
    owner: Owner,
}
impl RecordFiles for Private {
    fn create(&self, path: &Path) -> std::io::Result<File> {
        let file = focal_platform::fs::create_private_new(path, true, true)?;
        checked_private(path, &file, &self.owner)?;
        Ok(file)
    }
    fn open(&self, path: &Path, write: bool) -> std::io::Result<File> {
        if !focal_platform::fs::check_private_file(path, &self.owner, 1)? {
            return Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied));
        }
        let file = focal_platform::fs::open_private(path, true, write, false)?;
        checked_private(path, &file, &self.owner)?;
        Ok(file)
    }
}
/// `file`, opened at `path`, owner-private and the one at `path`.
fn checked_private(path: &Path, file: &File, owner: &Owner) -> std::io::Result<()> {
    if focal_platform::fs::check_private_file(path, owner, 1)?
        && focal_platform::fs::check_open_private_file(path, file, owner)?
    {
        Ok(())
    } else {
        Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied))
    }
}
/// A part's failure as the upload's: a file not private as it, one missing as corrupt.
fn part_error(error: ContentError) -> TransferError {
    match error {
        ContentError::Io(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            TransferError::Permissions
        }
        ContentError::Io(error) if error.kind() == std::io::ErrorKind::NotFound => {
            TransferError::Corrupt
        }
        ContentError::Io(error) => TransferError::Io(error),
        ContentError::Capacity => TransferError::Capacity,
        _ => TransferError::Corrupt,
    }
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Fault {
    FileSynced,
    Renamed,
}

type Owner = focal_platform::fs::Owner;
fn store_owner(path: &Path) -> Result<Owner, TransferError> {
    focal_platform::fs::owner_at(path).map_err(Into::into)
}
fn sync_dir(path: &Path) -> Result<(), TransferError> {
    #[cfg(unix)]
    {
        File::open(path)?.sync_all()?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}
fn parent(path: &Path) -> &Path {
    path.parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
}
fn present(path: &Path) -> Result<bool, TransferError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e.into()),
    }
}
fn missing(error: std::io::Error) -> TransferError {
    if error.kind() == std::io::ErrorKind::NotFound {
        TransferError::Corrupt
    } else {
        error.into()
    }
}

/// The durable bootstrap marker is outside the child store. Its lock is held
/// until the caller completes create/open, then released before network waits.
/// The marker names its context, so it is kept as `rest` says (29 §12.11):
/// sealed from the at-rest level, converted at the next bootstrap under its lock.
pub(super) fn bootstrap(
    parent: &Path,
    name: &str,
    context: crate::pending::OperationContext,
    rest: AtRest,
) -> Result<(FileLock, bool), TransferError> {
    if name.is_empty()
        || name.len() > 64
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_'))
        || context.cluster == [0; 16]
        || context.principal.is_zero()
        || context.ledger.tenant.is_zero()
        || context.ledger.session.is_zero()
    {
        return Err(TransferError::Invalid);
    }
    let owner = focal_platform::fs::private_dir_owner(parent)?.ok_or(TransferError::Permissions)?;
    let lock_path = parent.join(format!("{name}.lock"));
    let marker = parent.join(format!("{name}.initialized"));
    let first = !present(&lock_path)?;
    if first && (present(&marker)? || present(&parent.join(name))?) {
        return Err(TransferError::Corrupt);
    }
    if !first {
        check_file(&lock_path, &owner)?;
    }
    let lock = if first {
        focal_platform::fs::create_private_new(&lock_path, true, true)?
    } else {
        focal_platform::fs::open_private(&lock_path, true, true, false)?
    };
    check_open_file(&lock_path, &lock, &owner)?;
    let lock = FileLock::acquire(lock).map_err(|error| {
        if error.kind() == std::io::ErrorKind::WouldBlock {
            TransferError::Locked
        } else {
            error.into()
        }
    })?;
    let mut value = [0; 104];
    let mut cursor = std::io::Cursor::new(value.as_mut_slice());
    cursor.write_all(b"FCLUPBS1")?;
    for bytes in [
        &context.cluster,
        &context.principal.0,
        &context.ledger.tenant.0,
        &context.ledger.session.0,
    ] {
        cursor.write_all(bytes)?;
    }
    let checksum = *blake3::hash(value.get(..72).ok_or(TransferError::Corrupt)?).as_bytes();
    value
        .get_mut(72..)
        .ok_or(TransferError::Corrupt)?
        .copy_from_slice(&checksum);
    let file = OwnerFile::new(parent, &format!("{name}.initialized"), rest)?;
    if first {
        lock.file().sync_all()?;
        sync_dir(parent)?;
        if file.sealing() {
            file.mark()?;
        }
        let bytes = file.to_disk(value.to_vec())?;
        let mut written = focal_platform::fs::create_private_new(&marker, false, true)?;
        written.write_all(&bytes)?;
        written.sync_all()?;
        sync_dir(parent)?;
    } else {
        check_file(&marker, &owner)?;
        let mut read = focal_platform::fs::open_private(&marker, true, false, false)?;
        check_open_file(&marker, &read, &owner)?;
        let length = usize::try_from(read.metadata()?.len()).map_err(|_| TransferError::Corrupt)?;
        if length > file.bound(value.len())? {
            return Err(TransferError::Corrupt);
        }
        let mut saved = super::buffer(length)?;
        read.read_exact(&mut saved)?;
        drop(read);
        let opened = file.opened(saved.clone(), value.len())?;
        if opened.len() != value.len() {
            return Err(TransferError::Corrupt);
        }
        if opened != value {
            return Err(TransferError::Conflict);
        }
        file.convert(&saved, &opened)?;
    }
    Ok((lock, first))
}
fn check_file(path: &Path, owner: &Owner) -> Result<(), TransferError> {
    match focal_platform::fs::check_private_file(path, owner, 1) {
        Ok(true) => Ok(()),
        Ok(false) => Err(TransferError::Permissions),
        Err(error) => Err(missing(error)),
    }
}
fn check_open_file(path: &Path, file: &File, owner: &Owner) -> Result<(), TransferError> {
    check_file(path, owner)?;
    match focal_platform::fs::check_open_private_file(path, file, owner) {
        Ok(true) => Ok(()),
        Ok(false) => Err(TransferError::Permissions),
        Err(error) => Err(missing(error)),
    }
}
