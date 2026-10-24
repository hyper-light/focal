//! The content store at rest (29 §11): what its object files are sealed under and named by, and
//! the forms a store's files are in on the way there.
//!
//! - **A key per content domain.** Random, made when the domain's first object is installed, and
//!   kept beside its objects as its record under the node's content key ([`DOMAIN_KEY`], 61 bytes).
//!   Destroying that record erases every byte of the domain wherever it lies (NIST SP 800-88r1,
//!   cryptographic erase).
//! - **Files sealed by their content.** A chunk or a manifest is sealed whole under its domain's key
//!   with `hyper_seal::sealed_file::seal_content`: its data key and file ID derive from the domain
//!   key and the SHA-256 of exactly its bytes, taken by the writer, so the same bytes seal to the
//!   same file (an object installed again, a chunk two objects share) and no key seals two
//!   plaintexts (Bellare, Keelveedhi and Ristenpart, EUROCRYPT 2013, message-locked encryption
//!   within one parent). Every read still checks the opened bytes against the BLAKE3 hash that names
//!   them.
//! - **Names keyed per domain.** A file's name is the domain's keyed name of the hash it is filed
//!   under (`hyper_seal::name::Namer`, HMAC-SHA-256 under a child of the domain key, 128 bits), so a
//!   directory listing says nothing of what the domain holds, and equal names are equal hashes, as
//!   deduplication needs.
//! - **Keys held.** The store's writer holds the node's content key for its life. A domain's key and
//!   its naming key are unwrapped from its record for one file and dropped after it, two key slots
//!   for the moment, as a store's key is (`focal_seal::store_key`): nothing of a domain stays in
//!   memory. A reader holds no key: it unwraps the content key from its data directory's keys as it
//!   opens a domain's record, and drops it with the domain's.
//! - **Two forms, and the way between them.** Below the fence that opens the content level
//!   (focal-node's `upgrade::CONTENT_LEVEL`), a store's writer keeps its files in the clear and
//!   named by their hashes, byte for byte as every earlier binary did, so a node may still be rolled
//!   back to one ([`ContentForm::Clear`]). From the fence on, the writer seals whatever it installs
//!   ([`ContentForm::Sealed`]) and the files already there are converted while the store serves
//!   (`ContentStore::convert_step`): each is sealed under its keyed name, and only then is its name
//!   in the clear removed, so a reader that misses a file under the one name finds it under the
//!   other ([`DomainKey::opened`] looks in the clear first). The conversion marks the store as it
//!   begins ([`SEALING`]), so once begun it finishes whatever form a later start asks for, and as it
//!   ends ([`SEALED`]), after which nothing in the clear is looked for.
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use focal_model::{ContentDomainId, ContentHash};
use hyper_block::DiskError;
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_seal::SealError;
use hyper_seal::keys::{KeyId, RECORD, Wrapped, WrappingKey};
use hyper_seal::name::Namer;
use hyper_seal::sealed_file::{SealedFileError, SealedReader, SealedWriter, seal_content};
use hyper_seal::stream::VersionKey;

use super::ContentError;

/// Plaintext bytes of an object file's segment: what every whole file focal seals is cut into
/// (`focal_seal::file::SEGMENT`, 64 KiB, the segment hyper-seal's sealed-file bench measures); the
/// largest chunk, 1 MiB, is sixteen.
const SEGMENT: u32 = focal_seal::file::SEGMENT;
/// The file in a domain's directory that keeps the domain's key under the node's content key.
pub(crate) const DOMAIN_KEY: &str = "DOMAIN.key";
/// The file in a store's root that marks its conversion begun: from then on its writer seals,
/// whatever form a start asks for, until the conversion ends.
pub(crate) const SEALING: &str = "SEALING";
/// The file in a store's root that marks every object file sealed: nothing in the clear is looked
/// for after it.
pub(crate) const SEALED: &str = "SEALED";
/// What a domain's naming key is derived for (HKDF's salt and info, `derive_child`).
const NAMES: &[u8] = b"focal content names";
/// What a record file's key is derived for (HKDF's salt and info, `derive_child`), the file's ID
/// the label: a cold stream's open chunk, and a whole chunk cut from one (29 §12.6).
const RECORDS: &[u8] = b"focal cold record key";
/// Hex digits of a name in the clear: a BLAKE3 hash.
const CLEAR_NAME: usize = 64;
/// Hex digits of a keyed name: `hyper_seal::name`'s 128 bits.
const KEYED_NAME: usize = 32;

/// A name the collector marks a chunk by, fixed in size as a hash is (so a mark costs what it did
/// before the store sealed): the hash itself in the clear, the keyed name's 16 bytes and zeros
/// where the store seals.
pub(crate) type Mark = [u8; 32];

/// Bytes and the BLAKE3 hash taken of exactly them: what a seed or a cold chunk is written from,
/// so the hash its key is derived from and its name says is never a caller's word, where a wrong
/// one would put two plaintexts under one key and nonce (`FileSealer::content_keyed`'s contract).
#[derive(Clone, Copy)]
pub(crate) struct Hashed<'a> {
    bytes: &'a [u8],
    hash: ContentHash,
}

impl<'a> Hashed<'a> {
    pub(crate) fn of(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            hash: ContentHash(*blake3::hash(bytes).as_bytes()),
        }
    }
    pub(crate) fn hash(&self) -> ContentHash {
        self.hash
    }
    pub(crate) fn bytes(&self) -> &'a [u8] {
        self.bytes
    }
}

/// A seed's file on its way into place ([`DomainKey::seed_writer`]).
pub(crate) struct SeedWriter<'a> {
    seed: Hashed<'a>,
    temporary: PathBuf,
    sink: Sink,
}

enum Sink {
    Clear(File),
    /// Boxed: a writer is 704 bytes beside a file's handle, made once a seed.
    Sealed(Box<SealedWriter<DeviceFile>>),
}

impl SeedWriter<'_> {
    /// The seed's bytes written and its file finished, unflushed: the handle given back is the
    /// caller's to flush before it renames the file into place, as a batch of a checkpoint's seeds
    /// flushes its files together on threads of its own.
    pub(crate) fn finish(self) -> Result<File, ContentError> {
        match self.sink {
            Sink::Clear(mut file) => {
                file.write_all(self.seed.bytes)?;
                Ok(file)
            }
            Sink::Sealed(mut writer) => {
                writer.write(self.seed.bytes).map_err(sealed_error)?;
                let file = writer.finish_unflushed().map_err(sealed_error)?;
                // A handle of its own on the same file, for a flush on another thread.
                Ok(file.std_file().try_clone()?)
            }
        }
    }

    /// The seed installed at `path`: written, flushed, renamed into place and the directory
    /// flushed after (Pillai et al., OSDI 2014), so a crash leaves the old file or the new one
    /// whole.
    pub(crate) fn install(self, path: &Path) -> Result<(), ContentError> {
        let temporary = self.temporary.clone();
        let file = self.finish()?;
        file.sync_all()?;
        drop(file);
        focal_platform::fs::atomic_replace(&temporary, path)?;
        focal_platform::sync_dir(path.parent().ok_or(ContentError::Invalid)?)?;
        Ok(())
    }
}

/// The form a store's writer installs its object files in (29 §11).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentForm {
    /// In the clear, named by the hashes they are filed under, as every binary below the content
    /// level wrote them: a store's form while a binary that cannot open a sealed file may still be
    /// rolled back to.
    Clear,
    /// Sealed under their domains' keys and named by keyed hashes: from the fence on, and from
    /// genesis in a cluster founded there. A store in the clear is converted.
    Sealed,
}

/// What a file of a domain is: a chunk or an object's manifest, or a chunk of a session's
/// checkpoint seeds or cold stream, each directory of which is a domain of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Chunk,
    Manifest,
    Seed,
}

impl Kind {
    pub(crate) fn extension(self) -> &'static str {
        match self {
            Self::Chunk => "chunk",
            Self::Manifest => "manifest",
            Self::Seed => "seed",
        }
    }
}

/// The keys a store's files are written and read under, and the forms they may lie in.
pub(crate) struct ContentKeys<'a> {
    content: Content<'a>,
    /// The data directory's key a domain's record is kept under: the content key for the content
    /// store, the group key for a session's seeds and cold stream (`focal_seal::Store`).
    store: focal_seal::Store,
    /// Whether a file may lie in the clear: until the store is marked sealed.
    clear: bool,
}

/// Where the store's key comes from.
enum Content<'a> {
    /// A writer that keeps its files in the clear holds none.
    Clear,
    /// A writer that seals holds it for its life: the content store's one writer.
    Held(WrappingKey),
    /// A reader unwraps it from the data directory's keys as it opens a domain's record.
    Data(&'a focal_seal::DataKeys),
    /// A writer of a store a node keeps one of for each session (its seeds, its cold stream): the
    /// key unwrapped from the data directory's keys for each domain record it opens or makes and
    /// dropped after it, so a node's sessions hold no key slot between their writes, where a writer
    /// that held one would take a slot a session (`focal_seal::key_slots`).
    Owned(focal_seal::DataKeys),
}

/// A domain's key and its namer, where its record stands.
struct Sealed {
    key: WrappingKey,
    names: Namer,
}

/// What one file of a domain is written and looked for under.
pub(crate) struct DomainKey {
    /// The domain's key: none where its record does not stand, or where the writer seals nothing.
    sealed: Option<Sealed>,
    /// Whether a file of the domain may lie in the clear.
    clear: bool,
    /// Whether what the store's writer installs is sealed.
    seals: bool,
}

impl ContentKeys<'static> {
    /// A writer's keys that keep every file in the clear.
    pub(crate) fn clear() -> Self {
        Self {
            content: Content::Clear,
            store: focal_seal::Store::Content,
            clear: true,
        }
    }

    /// A writer's keys that seal: the content key of the data directory `keys` names, unwrapped now
    /// and held for the writer's life; `clear` while a file of the store may still lie in the clear.
    pub(crate) fn sealing(keys: &focal_seal::DataKeys, clear: bool) -> Result<Self, ContentError> {
        let content = keys.key(focal_seal::Store::Content).map_err(setup_error)?;
        Ok(Self {
            content: Content::Held(content),
            store: focal_seal::Store::Content,
            clear,
        })
    }

    /// A writer's keys that seal under `store`'s key of the data directory `keys` names, unwrapped
    /// for each domain record and never held ([`Content::Owned`]); `clear` while a file of the store
    /// may still lie in the clear.
    pub(crate) fn per_record(
        keys: focal_seal::DataKeys,
        store: focal_seal::Store,
        clear: bool,
    ) -> Self {
        Self {
            content: Content::Owned(keys),
            store,
            clear,
        }
    }

    /// The same keys for another writer of the store, such as a batch filled on another thread:
    /// a writer's that holds no key ([`Content::Clear`], [`Content::Owned`]); one that holds a key
    /// for its life has no copy, its key held once.
    pub(crate) fn owned_copy(&self) -> Result<ContentKeys<'static>, ContentError> {
        match &self.content {
            Content::Clear => Ok(ContentKeys::clear()),
            Content::Owned(keys) => Ok(ContentKeys::per_record(
                keys.clone(),
                self.store,
                self.clear,
            )),
            Content::Held(_) | Content::Data(_) => Err(ContentError::Invalid),
        }
    }

    /// A sealed store's writer keys over `content`.
    #[cfg(test)]
    pub(crate) fn new(content: WrappingKey) -> Self {
        Self {
            content: Content::Held(content),
            store: focal_seal::Store::Content,
            clear: false,
        }
    }
}

impl<'a> ContentKeys<'a> {
    /// A reader's keys over the data directory `keys` names; `clear` where the store it reads is not
    /// marked sealed.
    pub(crate) fn reading(keys: &'a focal_seal::DataKeys, clear: bool) -> Self {
        Self::reading_under(keys, focal_seal::Store::Content, clear)
    }

    /// A reader's keys over the data directory `keys` names, its domains' records kept under
    /// `store`'s key; `clear` where the store it reads is not marked sealed.
    pub(crate) fn reading_under(
        keys: &'a focal_seal::DataKeys,
        store: focal_seal::Store,
        clear: bool,
    ) -> Self {
        Self {
            content: Content::Data(keys),
            store,
            clear,
        }
    }

    /// Whether what the writer installs is sealed.
    pub(crate) fn seals(&self) -> bool {
        !matches!(self.content, Content::Clear)
    }

    /// Whether the store's files may lie in either form: it seals what it installs and is not yet
    /// marked sealed.
    pub(crate) fn converting(&self) -> bool {
        self.seals() && self.clear
    }

    /// Whether a file of the store may lie in the clear.
    pub(crate) fn clear_allowed(&self) -> bool {
        self.clear
    }

    /// The mark of a listed `stem`, where it is a name the writer files with
    /// ([`DomainKey::mark`]).
    pub(crate) fn mark_of(&self, stem: &str) -> Option<Mark> {
        if !self.seals() {
            return parse_hash(stem).map(|hash| hash.0);
        }
        if !is_hex(stem, KEYED_NAME) {
            return None;
        }
        let mut mark = [0u8; 32];
        for (index, byte) in mark.iter_mut().take(KEYED_NAME / 2).enumerate() {
            let pair = stem.get(index.checked_mul(2)?..index.checked_mul(2)?.checked_add(2)?)?;
            *byte = u8::from_str_radix(pair, 16).ok()?;
        }
        Some(mark)
    }

    /// The store is marked sealed: nothing in the clear is looked for from here on.
    pub(crate) fn mark_sealed(&mut self) {
        self.clear = false;
    }

    /// The key of `domain`, whose directory is `directory` in the store's own objects, unwrapped
    /// from its record where it stands; made, its record written first, where `create` is set, the
    /// writer seals and the domain has none. A domain whose record does not stand holds no sealed
    /// file.
    pub(crate) fn domain(
        &self,
        directory: &Path,
        domain: ContentDomainId,
        create: bool,
    ) -> Result<DomainKey, ContentError> {
        if let Content::Clear = self.content {
            return Ok(DomainKey {
                sealed: None,
                clear: true,
                seals: false,
            });
        }
        let path = directory.join(DOMAIN_KEY);
        let record = match read_record(&path) {
            Ok(record) => Some(record),
            Err(ContentError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                if create {
                    Some(self.make(&path)?)
                } else {
                    None
                }
            }
            Err(error) => return Err(error),
        };
        let sealed = match record {
            Some(record) => Some(self.open_record(&record, domain)?),
            None => None,
        };
        Ok(DomainKey {
            sealed,
            clear: self.clear,
            seals: true,
        })
    }

    /// `key`, as a writer that installs a file of `domain` needs it: its record made first where
    /// the writer seals and the domain has none ([`DomainKey::record_bytes`] is what that takes).
    pub(crate) fn writable(
        &self,
        directory: &Path,
        domain: ContentDomainId,
        key: DomainKey,
    ) -> Result<DomainKey, ContentError> {
        if key.record_bytes() == 0 {
            return Ok(key);
        }
        self.domain(directory, domain, true)
    }

    /// The domain's key unwrapped from its `record` under the content key, and its namer.
    fn open_record(
        &self,
        record: &Wrapped,
        domain: ContentDomainId,
    ) -> Result<Sealed, ContentError> {
        let secret = match &self.content {
            Content::Held(content) => content.unwrap(record).map_err(seal_error)?,
            Content::Data(keys) => keys
                .key(self.store)
                .map_err(setup_error)?
                .unwrap(record)
                .map_err(seal_error)?,
            Content::Owned(keys) => keys
                .key(self.store)
                .map_err(setup_error)?
                .unwrap(record)
                .map_err(seal_error)?,
            Content::Clear => return Err(ContentError::Invalid),
        };
        let key = WrappingKey::new(KeyId(domain.0), 0, secret);
        let (naming, _) = key.derive_child(NAMES, &domain.0).map_err(seal_error)?;
        let names = Namer::new(&naming).map_err(seal_error)?;
        Ok(Sealed { key, names })
    }

    /// A new domain key's record, written at `path` where none stands: its own temporary, flushed,
    /// then linked to the name no-clobber (`atomic_create_new`), the directory flushed after. A
    /// record that stood first is the domain's, and read instead. Only a writer makes one.
    fn make(&self, path: &Path) -> Result<Wrapped, ContentError> {
        let (_secret, record) = match &self.content {
            Content::Held(content) => content.make_child().map_err(seal_error)?,
            Content::Owned(keys) => keys
                .key(self.store)
                .map_err(setup_error)?
                .make_child()
                .map_err(seal_error)?,
            Content::Clear | Content::Data(_) => return Err(ContentError::Invalid),
        };
        let temporary = path.with_extension("key-install");
        fresh(&temporary)?;
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(&record.encode())?;
        file.sync_all()?;
        drop(file);
        let linked = focal_platform::fs::atomic_create_new(&temporary, path);
        let _ = fs::remove_file(&temporary);
        let directory = path.parent().ok_or(ContentError::Invalid)?;
        focal_platform::sync_dir(directory)?;
        match linked {
            Ok(()) => Ok(record),
            Err(_) if path.try_exists()? => read_record(path),
            Err(error) => Err(error.into()),
        }
    }
}

impl Sealed {
    fn name(&self, hash: ContentHash) -> Result<String, ContentError> {
        let name = self.names.name(&hash.0).map_err(seal_error)?;
        Ok(hex(&name))
    }

    fn mark(&self, hash: ContentHash) -> Result<Mark, ContentError> {
        let name = self.names.name(&hash.0).map_err(seal_error)?;
        let mut mark = [0u8; 32];
        let (head, _) = mark.split_at_mut(name.len().min(32));
        head.copy_from_slice(name.get(..head.len()).ok_or(ContentError::Invalid)?);
        Ok(mark)
    }
}

/// `temporary` made fresh before it is written: one a crash or an earlier failure left is removed,
/// so no byte of it outlives the write (a device file is opened without truncation, and a sealed
/// file longer than what was sealed into it would read as corrupt only when it is read). A removal
/// that fails other than for a file not there is the write's failure.
fn fresh(temporary: &Path) -> Result<(), ContentError> {
    match fs::remove_file(temporary) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

impl DomainKey {
    /// The key of a record file of the domain whose ID is `file`: derived from the domain's key
    /// and the ID (HKDF-SHA-384), so every record file has a key of its own, and held as the
    /// AES-GCM context of a [`VersionKey`], outside the locked region, for as long as its file is
    /// written or read. The domain's record must stand.
    pub(crate) fn record_key(&self, file: &[u8; 16]) -> Result<VersionKey, ContentError> {
        let sealed = self.sealed.as_ref().ok_or(ContentError::Invalid)?;
        let (secret, _) = sealed.key.derive_child(RECORDS, file).map_err(seal_error)?;
        VersionKey::new(&secret, *file).map_err(seal_error)
    }

    /// The sealed file at `path` read whole, at most `most` bytes, and checked against `hash`: a
    /// file another path found (a cold stream's pending chunk), sealed by [`Self::seed_writer`] or
    /// as the content store seals.
    pub(crate) fn read_sealed_at(
        &self,
        path: &Path,
        hash: ContentHash,
        most: usize,
    ) -> Result<Vec<u8>, ContentError> {
        self.read_bounded(path, true, hash, most)
    }

    /// `names` written at `temporary` as one file's bytes, each name's 32 in turn, and left
    /// unflushed: in the clear, or sealed keyed by their BLAKE3 hash, which is taken here over
    /// exactly the names written (two passes over the one slice, so no hash is a caller's word).
    /// The hash, and a handle the caller flushes before it renames the file into place: a cold
    /// stream's manifest chunk, cut from the names it holds.
    pub(crate) fn write_names_unflushed(
        &self,
        temporary: &Path,
        names: &[ContentHash],
    ) -> Result<(ContentHash, File), ContentError> {
        let mut hasher = blake3::Hasher::new();
        for name in names {
            hasher.update(&name.0);
        }
        let hash = ContentHash(*hasher.finalize().as_bytes());
        if !self.seals {
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(temporary)?;
            // A page of names at a time.
            let mut page = [0u8; 4096];
            for group in names.chunks(page.len() / 32) {
                let mut filled = 0usize;
                for name in group {
                    let end = filled.checked_add(32).ok_or(ContentError::Capacity)?;
                    page.get_mut(filled..end)
                        .ok_or(ContentError::Capacity)?
                        .copy_from_slice(&name.0);
                    filled = end;
                }
                file.write_all(page.get(..filled).ok_or(ContentError::Capacity)?)?;
            }
            return Ok((hash, file));
        }
        let sealed = self.sealed.as_ref().ok_or(ContentError::Invalid)?;
        fresh(temporary)?;
        let file = DeviceFile::open(temporary, true, CachingRequest::Buffered, Alignment::BYTE)
            .map_err(disk_error)?;
        let mut writer = SealedWriter::content_keyed(file, &sealed.key, &hash.0, SEGMENT)
            .map_err(sealed_error)?;
        for name in names {
            writer.write(&name.0).map_err(sealed_error)?;
        }
        let file = writer.finish_unflushed().map_err(sealed_error)?;
        Ok((hash, file.std_file().try_clone()?))
    }

    /// Whether what the writer installs in the domain is sealed.
    pub(crate) fn seals(&self) -> bool {
        self.seals
    }

    /// Whether the domain holds no file: nothing of it may lie in the clear and its record does not
    /// stand.
    pub(crate) fn holds_nothing(&self) -> bool {
        self.sealed.is_none() && !self.clear
    }

    /// The bytes a writer's first install in the domain adds for its key's record: a record's where
    /// the writer seals and none stands yet.
    pub(crate) fn record_bytes(&self) -> u64 {
        if self.seals && self.sealed.is_none() {
            RECORD as u64
        } else {
            0
        }
    }

    /// The bytes a file of `plain` bytes takes on its volume as the writer installs it: sealed
    /// whole (`focal_seal::file::sealed_bound`), or as they are in the clear.
    pub(crate) fn file_bytes(&self, plain: usize) -> Result<u64, ContentError> {
        if self.seals {
            focal_seal::file::sealed_bound(plain)
                .and_then(|bytes| u64::try_from(bytes).ok())
                .ok_or(ContentError::Capacity)
        } else {
            u64::try_from(plain).map_err(|_| ContentError::Capacity)
        }
    }

    /// The name the writer files a file filed under `hash` with: the domain's keyed name of it in
    /// hex where the writer seals, the hash itself where it keeps the clear.
    pub(crate) fn name(&self, hash: ContentHash) -> Result<String, ContentError> {
        if !self.seals {
            return Ok(hash.to_string());
        }
        self.sealed
            .as_ref()
            .ok_or(ContentError::Invalid)?
            .name(hash)
    }

    /// The collector's mark of the file filed under `hash`: what [`ContentKeys::mark_of`] reads
    /// back from its name.
    pub(crate) fn mark(&self, hash: ContentHash) -> Result<Mark, ContentError> {
        if !self.seals {
            return Ok(hash.0);
        }
        self.sealed
            .as_ref()
            .ok_or(ContentError::Invalid)?
            .mark(hash)
    }

    /// Whether `stem` is a name the writer files with: what a pass over a directory considers, and
    /// leaves anything else as it is.
    pub(crate) fn is_name(&self, stem: &str) -> bool {
        let width = if self.seals { KEYED_NAME } else { CLEAR_NAME };
        is_hex(stem, width)
    }

    /// Where the writer installs the file filed under `hash` in `directory`.
    pub(crate) fn path(
        &self,
        directory: &Path,
        hash: ContentHash,
        kind: Kind,
    ) -> Result<PathBuf, ContentError> {
        Ok(directory.join(format!("{}.{}", self.name(hash)?, kind.extension())))
    }

    /// `bytes` installed at `path`: written into its own temporary as they are, or sealed by their
    /// content as written, flushed, renamed into place, the directory flushed after (Pillai et al.,
    /// OSDI 2014), so a crash leaves the old file or the new one whole.
    pub(crate) fn install(&self, path: &Path, bytes: &[u8]) -> Result<(), ContentError> {
        if !self.seals {
            return super::atomic_install(path, bytes);
        }
        let sealed = self.sealed.as_ref().ok_or(ContentError::Invalid)?;
        let temporary = path.with_extension("install");
        fresh(&temporary)?;
        let file = DeviceFile::open(&temporary, true, CachingRequest::Buffered, Alignment::BYTE)
            .map_err(disk_error)?;
        let file = seal_content(file, &sealed.key, bytes, SEGMENT).map_err(sealed_error)?;
        drop(file);
        focal_platform::fs::atomic_replace(&temporary, path)?;
        focal_platform::sync_dir(path.parent().ok_or(ContentError::Invalid)?)?;
        Ok(())
    }

    /// A writer of `seed` into `temporary`, made under the domain's key, which the caller lets go
    /// before the bytes are written: in the clear as they are, or sealed keyed by the seed's hash
    /// (`SealedWriter::content_keyed`), the BLAKE3 hash the store names it by, so its bytes are
    /// passed over once. The writer seals with a key of its own, derived as it is made, and holds
    /// the seed itself, so what it writes is always the plaintext its key was derived for.
    pub(crate) fn seed_writer<'a>(
        &self,
        temporary: &Path,
        seed: Hashed<'a>,
    ) -> Result<SeedWriter<'a>, ContentError> {
        let sink = if self.seals {
            let sealed = self.sealed.as_ref().ok_or(ContentError::Invalid)?;
            fresh(temporary)?;
            let file = DeviceFile::open(temporary, true, CachingRequest::Buffered, Alignment::BYTE)
                .map_err(disk_error)?;
            Sink::Sealed(Box::new(
                SealedWriter::content_keyed(file, &sealed.key, &seed.hash.0, SEGMENT)
                    .map_err(sealed_error)?,
            ))
        } else {
            Sink::Clear(
                OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .open(temporary)?,
            )
        };
        Ok(SeedWriter {
            seed,
            temporary: temporary.to_path_buf(),
            sink,
        })
    }

    /// The file filed under `hash` in `directory` opened into the head of `scratch`, which must hold
    /// exactly `length` bytes of plaintext, and checked against `hash`: the verified bytes, borrowed
    /// from the scratch. A file of another plaintext length, one that does not open, or bytes that
    /// hash otherwise are corrupt.
    pub(crate) fn open_into<'s>(
        &self,
        directory: &Path,
        hash: ContentHash,
        kind: Kind,
        length: usize,
        scratch: &'s mut [u8],
    ) -> Result<&'s [u8], ContentError> {
        Ok(self.opened(directory, hash, kind, length, scratch)?.1)
    }

    /// [`Self::open_into`], with the path the file was opened at.
    ///
    /// A file may lie in the clear while the store is not marked sealed, and under its keyed name
    /// where the domain's record stands: the name in the clear is looked at first, since a file
    /// converted is sealed before its name in the clear goes, so a reader that misses it there
    /// finds it under the other, whatever the conversion does between. A file in the clear that
    /// does not read whole gives way to its sealed form where one opens (the writer seals over a
    /// damaged file while the store converts); where none does, its own refusal is the answer.
    pub(crate) fn opened<'s>(
        &self,
        directory: &Path,
        hash: ContentHash,
        kind: Kind,
        length: usize,
        scratch: &'s mut [u8],
    ) -> Result<(PathBuf, &'s [u8]), ContentError> {
        let block = scratch.get_mut(..length).ok_or(ContentError::Capacity)?;
        let (path, ()) = self.locate(directory, hash, kind, |path, sealed| {
            self.read_whole(path, sealed, hash, block)
        })?;
        Ok((path, block))
    }

    /// The file filed under `hash` in `directory` read whole, at most `most` bytes, and checked
    /// against `hash`: one open, its length taken from what was opened, where a length asked first
    /// and then the file opened again would open it twice (a manifest's read).
    pub(crate) fn read_sized(
        &self,
        directory: &Path,
        hash: ContentHash,
        kind: Kind,
        most: usize,
    ) -> Result<Vec<u8>, ContentError> {
        Ok(self
            .locate(directory, hash, kind, |path, sealed| {
                self.read_bounded(path, sealed, hash, most)
            })?
            .1)
    }

    /// The plaintext length of the file filed under `hash` in `directory`, nothing opened past a
    /// sealed file's header: what a reader charges before it reads one.
    pub(crate) fn plain_length(
        &self,
        directory: &Path,
        hash: ContentHash,
        kind: Kind,
    ) -> Result<u64, ContentError> {
        Ok(self
            .locate(directory, hash, kind, |path, sealed| {
                self.length_of(path, sealed)
            })?
            .1)
    }

    /// Whether the sealed file at `path` opens to the bytes `hash` names, read into the head of
    /// `scratch` at its stated length: what a conversion asks of a file it finds sealed already
    /// before it lets the file's name in the clear go. A file not there, or one that does not open
    /// to them, does not hold them.
    pub(crate) fn holds_sealed(
        &self,
        path: &Path,
        hash: ContentHash,
        scratch: &mut [u8],
    ) -> Result<bool, ContentError> {
        let length = match self.length_of(path, true) {
            Ok(length) => length,
            Err(ContentError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(false);
            }
            Err(ContentError::Corrupt) => return Ok(false),
            Err(error) => return Err(error),
        };
        let Some(block) = usize::try_from(length)
            .ok()
            .and_then(|length| scratch.get_mut(..length))
        else {
            return Ok(false);
        };
        match self.read_whole(path, true, hash, block) {
            Ok(()) => Ok(true),
            Err(ContentError::Corrupt) => Ok(false),
            Err(ContentError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    /// Whether a file filed under `hash` lies in `directory`, in either form.
    pub(crate) fn exists(
        &self,
        directory: &Path,
        hash: ContentHash,
        kind: Kind,
    ) -> Result<bool, ContentError> {
        Ok(self.existing(directory, hash, kind)?.is_some())
    }

    /// The path of the file filed under `hash` in `directory`, in whichever form lies there first.
    pub(crate) fn existing(
        &self,
        directory: &Path,
        hash: ContentHash,
        kind: Kind,
    ) -> Result<Option<PathBuf>, ContentError> {
        for (path, _) in self
            .candidates(directory, hash, kind)?
            .into_iter()
            .flatten()
        {
            if path.try_exists()? {
                return Ok(Some(path));
            }
        }
        Ok(None)
    }

    /// A file a pass over a directory lists at `path`, opened whole, at most `most` bytes, where
    /// the hash it is filed under is not yet known (the collector's pass over a domain): its bytes
    /// and that hash; none where its name is not one the writer files with, or it is gone since it
    /// was listed. Sealed, the hash is the BLAKE3 of what opened, and a file whose name is not that
    /// hash's keyed name is corrupt, so a file put under another name opens to nothing; in the
    /// clear, its name is its hash, as every earlier binary read it.
    pub(crate) fn open_listed(
        &self,
        path: &Path,
        most: usize,
    ) -> Result<Option<(Vec<u8>, ContentHash)>, ContentError> {
        let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
            return Ok(None);
        };
        if !self.is_name(stem) {
            return Ok(None);
        }
        if !self.seals {
            let Some(hash) = parse_hash(stem) else {
                return Ok(None);
            };
            return match super::read_bounded(path, most) {
                Ok(bytes) => Ok(Some((bytes, hash))),
                Err(ContentError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                    Ok(None)
                }
                Err(error) => Err(error),
            };
        }
        let sealed = self.sealed.as_ref().ok_or(ContentError::Invalid)?;
        let file = match DeviceFile::open(path, false, CachingRequest::Buffered, Alignment::BYTE)
            .map_err(disk_error)
        {
            Ok(file) => file,
            Err(ContentError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        let mut reader = SealedReader::open(file, &sealed.key, SEGMENT).map_err(sealed_error)?;
        let length = usize::try_from(reader.len())
            .ok()
            .filter(|length| *length <= most)
            .ok_or(ContentError::Capacity)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length)
            .map_err(|_| ContentError::Capacity)?;
        bytes.resize(length, 0);
        reader.read_at(&mut bytes, 0).map_err(sealed_error)?;
        let hash = ContentHash(*blake3::hash(&bytes).as_bytes());
        if stem != sealed.name(hash)? {
            return Err(ContentError::Corrupt);
        }
        Ok(Some((bytes, hash)))
    }

    /// Where a file filed under `hash` may lie in `directory`, in the order it is looked for: its
    /// name in the clear while the store may hold one, then its keyed name where the domain's record
    /// stands; each with whether it is sealed.
    fn candidates(
        &self,
        directory: &Path,
        hash: ContentHash,
        kind: Kind,
    ) -> Result<[Option<(PathBuf, bool)>; 2], ContentError> {
        let clear = self.clear.then(|| {
            (
                directory.join(format!("{hash}.{}", kind.extension())),
                false,
            )
        });
        let sealed = match &self.sealed {
            Some(sealed) => Some((
                directory.join(format!("{}.{}", sealed.name(hash)?, kind.extension())),
                true,
            )),
            None => None,
        };
        Ok([clear, sealed])
    }

    /// `attempt` at each place the file may lie ([`Self::candidates`]): the first that succeeds,
    /// with its path. A failure in the clear gives way to the sealed form; where that is not there
    /// either, the failure in the clear is the answer, and a file in neither place is not found.
    fn locate<T>(
        &self,
        directory: &Path,
        hash: ContentHash,
        kind: Kind,
        mut attempt: impl FnMut(&Path, bool) -> Result<T, ContentError>,
    ) -> Result<(PathBuf, T), ContentError> {
        let [clear, sealed] = self.candidates(directory, hash, kind)?;
        let mut missed = None;
        if let Some((path, _)) = clear {
            match attempt(&path, false) {
                Ok(value) => return Ok((path, value)),
                Err(error) if sealed.is_some() => missed = Some(error),
                Err(error) => return Err(error),
            }
        }
        let Some((path, _)) = sealed else {
            return Err(missed.unwrap_or_else(not_found));
        };
        match attempt(&path, true) {
            Ok(value) => Ok((path, value)),
            Err(ContentError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                Err(missed.unwrap_or(ContentError::Io(error)))
            }
            Err(error) => Err(error),
        }
    }

    /// The file at `path` read whole into `block`, exactly its length, and checked against `hash`.
    fn read_whole(
        &self,
        path: &Path,
        sealed: bool,
        hash: ContentHash,
        block: &mut [u8],
    ) -> Result<(), ContentError> {
        if sealed {
            let key = &self.sealed.as_ref().ok_or(ContentError::Invalid)?.key;
            let file = DeviceFile::open(path, false, CachingRequest::Buffered, Alignment::BYTE)
                .map_err(disk_error)?;
            let mut reader = SealedReader::open(file, key, SEGMENT).map_err(sealed_error)?;
            if reader.len() != block.len() as u64 {
                return Err(ContentError::Corrupt);
            }
            reader.read_at(block, 0).map_err(sealed_error)?;
        } else {
            let mut file = File::open(path)?;
            if file.metadata()?.len() != block.len() as u64 {
                return Err(ContentError::Corrupt);
            }
            file.read_exact(block)?;
            // The file must be exactly its length: a byte appended since the length was read is
            // corruption.
            let mut extra = [0u8; 1];
            if file.read(&mut extra)? != 0 {
                return Err(ContentError::Corrupt);
            }
        }
        if ContentHash(*blake3::hash(block).as_bytes()) != hash {
            return Err(ContentError::Corrupt);
        }
        Ok(())
    }

    /// The file at `path` read whole into a buffer of exactly its length, at most `most` bytes,
    /// and checked against `hash`.
    fn read_bounded(
        &self,
        path: &Path,
        sealed: bool,
        hash: ContentHash,
        most: usize,
    ) -> Result<Vec<u8>, ContentError> {
        let exact = |length: u64| {
            let length = usize::try_from(length)
                .ok()
                .filter(|length| *length <= most)
                .ok_or(ContentError::Capacity)?;
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(length)
                .map_err(|_| ContentError::Capacity)?;
            bytes.resize(length, 0);
            Ok::<_, ContentError>(bytes)
        };
        let bytes = if sealed {
            let key = &self.sealed.as_ref().ok_or(ContentError::Invalid)?.key;
            let file = DeviceFile::open(path, false, CachingRequest::Buffered, Alignment::BYTE)
                .map_err(disk_error)?;
            let mut reader = SealedReader::open(file, key, SEGMENT).map_err(sealed_error)?;
            let mut bytes = exact(reader.len())?;
            reader.read_at(&mut bytes, 0).map_err(sealed_error)?;
            bytes
        } else {
            let mut file = File::open(path)?;
            let mut bytes = exact(file.metadata()?.len())?;
            file.read_exact(&mut bytes)?;
            let mut extra = [0u8; 1];
            if file.read(&mut extra)? != 0 {
                return Err(ContentError::Corrupt);
            }
            bytes
        };
        if ContentHash(*blake3::hash(&bytes).as_bytes()) != hash {
            return Err(ContentError::Corrupt);
        }
        Ok(bytes)
    }

    /// The plaintext length of the file at `path`: a sealed file's from its stated stream, one in
    /// the clear its own.
    fn length_of(&self, path: &Path, sealed: bool) -> Result<u64, ContentError> {
        if !sealed {
            return Ok(fs::metadata(path)?.len());
        }
        let key = &self.sealed.as_ref().ok_or(ContentError::Invalid)?.key;
        let file = DeviceFile::open(path, false, CachingRequest::Buffered, Alignment::BYTE)
            .map_err(disk_error)?;
        let reader = SealedReader::open(file, key, SEGMENT).map_err(sealed_error)?;
        Ok(reader.len())
    }
}

/// The hash a name in the clear is: 64 lowercase hex digits.
pub(crate) fn parse_hash(name: &str) -> Option<ContentHash> {
    if !is_hex(name, CLEAR_NAME) {
        return None;
    }
    let mut bytes = [0u8; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        let pair = name.get(index.checked_mul(2)?..index.checked_mul(2)?.checked_add(2)?)?;
        *byte = u8::from_str_radix(pair, 16).ok()?;
    }
    Some(ContentHash(bytes))
}

/// Whether `name` is `width` lowercase hex digits, as every name the store writes is.
fn is_hex(name: &str, width: usize) -> bool {
    name.len() == width
        && name
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn not_found() -> ContentError {
    ContentError::Io(std::io::Error::from(std::io::ErrorKind::NotFound))
}

/// The domain record at `path`: exactly a record's bytes, decoded.
fn read_record(path: &Path) -> Result<Wrapped, ContentError> {
    let mut file = File::open(path)?;
    let mut bytes = [0u8; RECORD];
    file.read_exact(&mut bytes)?;
    let mut extra = [0u8; 1];
    if file.read(&mut extra)? != 0 {
        return Err(ContentError::Corrupt);
    }
    Wrapped::decode(&bytes).map_err(|_| ContentError::Corrupt)
}

fn setup_error(error: focal_seal::SealSetupError) -> ContentError {
    match error {
        focal_seal::SealSetupError::Seal(error) => seal_error(error),
        focal_seal::SealSetupError::Io(error) => ContentError::Io(error),
        focal_seal::SealSetupError::KeyFile { source, .. } => ContentError::Io(source),
        // A directory without keys, or keys that do not open: no object of it can be sealed or
        // read until its owner's start has made or found them.
        _ => ContentError::Invalid,
    }
}

fn seal_error(error: SealError) -> ContentError {
    match error {
        // No key slot free in the process's locked region: refused for the moment.
        SealError::Capacity => ContentError::Capacity,
        SealError::Random => ContentError::Io(std::io::Error::other(
            "the operating system's random source failed",
        )),
        _ => ContentError::Corrupt,
    }
}

fn sealed_error(error: SealedFileError) -> ContentError {
    match error {
        SealedFileError::Seal(error) => seal_error(error),
        SealedFileError::Disk(error) => disk_error(error),
        SealedFileError::Layout(_) => ContentError::Corrupt,
        SealedFileError::Bound(_) => ContentError::Capacity,
    }
}

fn disk_error(error: DiskError) -> ContentError {
    match error {
        DiskError::Io { source, .. } => ContentError::Io(source),
        // A file shorter than its stated layout was cut by someone, never by a crash: sealed files
        // are renamed into place only once flushed.
        DiskError::ShortRead { .. } => ContentError::Corrupt,
        other => ContentError::Io(std::io::Error::other(other.to_string())),
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut hex = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

#[cfg(test)]
#[path = "sealing_tests.rs"]
mod tests;
