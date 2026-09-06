use super::{MAX_STATE_BYTES, TransferError};
use fs2::FileExt;
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

const MARKER: &str = "INITIALIZED";
const PAYLOAD: &str = "payload.bin";
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
    _lock: File,
    layout: Layout,
    #[cfg(test)]
    fault: std::cell::Cell<Option<Fault>>,
}
impl Directory {
    pub(super) fn path(&self) -> &Path {
        &self.path
    }
    pub(super) fn create(path: &Path) -> Result<(Self, File), TransferError> {
        let directory = Self::create_layout(path, Layout::Upload)?;
        let payload = options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path.join(PAYLOAD))?;
        directory.check_open(&path.join(PAYLOAD), &payload)?;
        payload.sync_all()?;
        File::open(path)?.sync_all()?;
        Ok((directory, payload))
    }
    pub(super) fn create_catalogue(path: &Path) -> Result<Self, TransferError> {
        Self::create_layout(path, Layout::Catalogue)
    }
    pub(super) fn open_catalogue(path: &Path) -> Result<Self, TransferError> {
        let directory = Self::lock(path, false, Layout::Catalogue)?;
        directory.read()?;
        directory.recover_marker()?;
        Ok(directory)
    }
    fn create_layout(path: &Path, layout: Layout) -> Result<Self, TransferError> {
        #[cfg(not(unix))]
        {
            let _ = (path, layout);
            Err(TransferError::Permissions)
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            fs::DirBuilder::new()
                .mode(0o700)
                .create(path)
                .map_err(|e| {
                    if e.kind() == std::io::ErrorKind::AlreadyExists {
                        TransferError::Exists
                    } else {
                        e.into()
                    }
                })?;
            File::open(parent(path))?.sync_all()?;
            Self::lock(path, true, layout)
        }
    }
    pub(super) fn open(path: &Path) -> Result<(Self, File), TransferError> {
        let directory = Self::lock(path, false, Layout::Upload)?;
        let payload = directory.open_file(PAYLOAD, true)?;
        Ok((directory, payload))
    }
    fn lock(path: &Path, create: bool, layout: Layout) -> Result<Self, TransferError> {
        #[cfg(not(unix))]
        {
            let _ = (path, create, layout);
            Err(TransferError::Permissions)
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let metadata = fs::symlink_metadata(path).map_err(missing)?;
            if !metadata.is_dir() || metadata.mode() & 0o077 != 0 {
                return Err(TransferError::Permissions);
            }
            let lock_path = path.join("LOCK");
            if !create {
                check_path(&lock_path, metadata.uid())?;
            }
            let lock = options()
                .read(true)
                .write(true)
                .create_new(create)
                .open(&lock_path)
                .map_err(missing)?;
            check_open(&lock_path, &lock, metadata.uid())?;
            lock.try_lock_exclusive().map_err(|e| {
                if e.kind() == std::io::ErrorKind::WouldBlock {
                    TransferError::Locked
                } else {
                    e.into()
                }
            })?;
            if create {
                lock.sync_all()?;
                File::open(path)?.sync_all()?;
            }
            Ok(Self {
                path: path.into(),
                _lock: lock,
                layout,
                #[cfg(test)]
                fault: std::cell::Cell::new(None),
            })
        }
    }
    fn open_file(&self, name: &str, write: bool) -> Result<File, TransferError> {
        let path = self.path.join(name);
        self.check_path(&path)?;
        let file = options()
            .read(true)
            .write(write)
            .open(&path)
            .map_err(missing)?;
        self.check_open(&path, &file)?;
        Ok(file)
    }
    fn check_path(&self, path: &Path) -> Result<(), TransferError> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            check_path(path, fs::metadata(&self.path)?.uid()).map(|_| ())
        }
        #[cfg(not(unix))]
        {
            let _ = path;
            Err(TransferError::Permissions)
        }
    }
    fn check_open(&self, path: &Path, file: &File) -> Result<(), TransferError> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            check_open(path, file, fs::metadata(&self.path)?.uid())
        }
        #[cfg(not(unix))]
        {
            let _ = (path, file);
            Err(TransferError::Permissions)
        }
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
        if !(44..=maximum).contains(&length) {
            return Err(TransferError::Corrupt);
        }
        let mut bytes = super::buffer(length)?;
        file.read_exact(&mut bytes)?;
        let mut extra = [0; 1];
        if file.read(&mut extra)? != 0 || bytes.get(..8) != Some(self.layout.magic().as_slice()) {
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
        let mut file = options().write(true).create_new(true).open(&temporary)?;
        let length = u32::try_from(payload.len())
            .map_err(|_| TransferError::Capacity)?
            .to_be_bytes();
        let mut hash = blake3::Hasher::new();
        hash.update(self.layout.magic());
        hash.update(&length);
        hash.update(payload);
        file.write_all(self.layout.magic())?;
        file.write_all(&length)?;
        file.write_all(payload)?;
        file.write_all(hash.finalize().as_bytes())?;
        file.sync_all()?;
        #[cfg(test)]
        self.fail_at(Fault::FileSynced)?;
        // The private exclusive lock protects both initial and replacement
        // publication; rename avoids a two-link crash window entirely.
        fs::rename(&temporary, &state)?;
        #[cfg(test)]
        self.fail_at(Fault::Renamed)?;
        File::open(&self.path)?.sync_all()?;
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
        self.read()?;
        self.open_file(self.layout.record(), false)?.sync_all()?;
        let marker = self.path.join(MARKER);
        if present(&marker)? {
            return self.check_marker();
        }
        let mut file = options().write(true).create_new(true).open(&marker)?;
        file.write_all(self.layout.magic())?;
        file.sync_all()?;
        File::open(&self.path)?.sync_all()?;
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
#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Fault {
    FileSynced,
    Renamed,
}

fn options() -> OpenOptions {
    let mut options = OpenOptions::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
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
pub(super) fn bootstrap(
    parent: &Path,
    name: &str,
    context: crate::pending::OperationContext,
) -> Result<(File, bool), TransferError> {
    #[cfg(not(unix))]
    {
        let _ = (parent, name, context);
        Err(TransferError::Permissions)
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
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
        let metadata = fs::symlink_metadata(parent)?;
        if !metadata.is_dir() || metadata.mode() & 0o077 != 0 {
            return Err(TransferError::Permissions);
        }
        let lock_path = parent.join(format!("{name}.lock"));
        let marker = parent.join(format!("{name}.initialized"));
        let first = !present(&lock_path)?;
        if first && (present(&marker)? || present(&parent.join(name))?) {
            return Err(TransferError::Corrupt);
        }
        if !first {
            check_path(&lock_path, metadata.uid())?;
        }
        let lock = options()
            .read(true)
            .write(true)
            .create_new(first)
            .open(&lock_path)?;
        check_open(&lock_path, &lock, metadata.uid())?;
        lock.try_lock_exclusive().map_err(|error| {
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
        if first {
            lock.sync_all()?;
            File::open(parent)?.sync_all()?;
            let mut file = options().write(true).create_new(true).open(&marker)?;
            file.write_all(&value)?;
            file.sync_all()?;
            File::open(parent)?.sync_all()?;
        } else {
            check_path(&marker, metadata.uid())?;
            let mut file = options().read(true).open(&marker)?;
            check_open(&marker, &file, metadata.uid())?;
            if file.metadata()?.len() != 104 {
                return Err(TransferError::Corrupt);
            }
            let mut saved = [0; 104];
            file.read_exact(&mut saved)?;
            if saved != value {
                return Err(TransferError::Conflict);
            }
        }
        Ok((lock, first))
    }
}
#[cfg(unix)]
fn check_path(path: &Path, uid: u32) -> Result<std::fs::Metadata, TransferError> {
    use std::os::unix::fs::MetadataExt;
    let metadata = fs::symlink_metadata(path).map_err(missing)?;
    if !metadata.is_file()
        || metadata.mode() & 0o077 != 0
        || metadata.uid() != uid
        || metadata.nlink() != 1
    {
        return Err(TransferError::Permissions);
    }
    Ok(metadata)
}
#[cfg(unix)]
fn check_open(path: &Path, file: &File, uid: u32) -> Result<(), TransferError> {
    use std::os::unix::fs::MetadataExt;
    let metadata = check_path(path, uid)?;
    let opened = file.metadata()?;
    if opened.dev() != metadata.dev()
        || opened.ino() != metadata.ino()
        || opened.uid() != uid
        || opened.mode() & 0o077 != 0
        || opened.nlink() != 1
        || !opened.is_file()
    {
        return Err(TransferError::Permissions);
    }
    Ok(())
}
