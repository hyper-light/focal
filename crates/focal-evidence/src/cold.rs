//! A session's cold stream (doc 31 M2, A2): the families it retired, as their archive frames, in
//! the order its log retired them. Every replica appends the same frames where the same records
//! apply, so every replica's stream holds the same bytes, and it is cut into chunks at the same
//! offsets, so every replica's chunks hash alike: an installing replica takes any replica's chunks
//! by their hashes, and a chunk that differs from a peer's is a divergence found.
//!
//! The stream past its last whole chunk is the open chunk, a file appended in place and never
//! synced on its own: a checkpoint carries its bytes. A whole chunk is renamed under its hash as
//! `.pending`, unsynced; a checkpoint makes the chunks it names durable before it may be a
//! recovery base ([`ColdStore::make_durable`]): until then recovery starts from an earlier one,
//! whose image still holds the retired rows, and replay appends the same frames and cuts the same
//! chunks again. So no family costs a sync, and the stream costs a session no memory but its
//! names, charged to its budget. What an append takes (the volume of every chunk it cuts, the room
//! to name them) is promised before it begins ([`ColdStore::room`]), so an append fails only on
//! IO: a replica that applied a record never finds its batch refused for room. Retired data is
//! kept: nothing here is swept, and no seed sweep reaches this directory.
use super::*;
use focal_memory::{Allocation, BudgetKind, MemoryBudget};

/// How a cold stream is cut: whole chunks of `chunk_bytes`, and a manifest chunk of the names of
/// every `manifest_names` of them. Every replica of a session cuts its stream alike.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColdGeometry {
    pub chunk_bytes: usize,
    pub manifest_names: usize,
}
impl ColdGeometry {
    /// A mebibyte a chunk, as a seed chunk; 4,096 names a manifest chunk (128 KiB, naming 4 GiB of
    /// stream).
    pub const STANDARD: Self = Self {
        chunk_bytes: SEED_CHUNK_BYTES,
        manifest_names: 4096,
    };
    /// The bytes a manifest chunk holds: 32 a name.
    fn manifest_bytes(self) -> Result<usize, ContentError> {
        self.manifest_names
            .checked_mul(32)
            .ok_or(ContentError::Capacity)
    }
    /// The most memory a read of `length` bytes of a stream holds at once: the bytes, a whole
    /// chunk read to check it against its name, and a manifest chunk with the names it holds,
    /// read to find that name.
    pub fn read_bytes(self, length: usize) -> Result<usize, ContentError> {
        let manifest = self.manifest_bytes()?;
        length
            .checked_add(self.chunk_bytes)
            .and_then(|n| n.checked_add(manifest))
            .and_then(|n| n.checked_add(manifest))
            .ok_or(ContentError::Capacity)
    }
    /// This geometry, if a chunk and a manifest chunk of it fit a seed chunk.
    pub fn valid(self) -> Result<Self, ContentError> {
        if self.chunk_bytes == 0
            || self.chunk_bytes > SEED_CHUNK_BYTES
            || self.manifest_names < 2
            || self.manifest_bytes()? > SEED_CHUNK_BYTES
        {
            return Err(ContentError::Invalid);
        }
        Ok(self)
    }
}

/// What a checkpoint names of a cold stream: its length, its whole chunks by their hashes (those
/// of each manifest's worth through a manifest chunk) and the open chunk's bytes. Every byte it
/// names is verifiable by a hash it carries, but the open chunk, which it carries whole.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColdState {
    /// The stream's length.
    pub length: u64,
    /// The manifest chunks, in order, each naming a manifest's worth of chunks.
    pub manifests: Vec<ContentHash>,
    /// The whole chunks after the last manifest chunk's, in order: fewer than a manifest holds.
    pub names: Vec<ContentHash>,
    /// The stream past its last whole chunk: less than a chunk.
    pub open: Vec<u8>,
}
impl ColdState {
    /// How many whole chunks it names.
    fn chunks_named(&self, geometry: ColdGeometry) -> Result<usize, ContentError> {
        self.manifests
            .len()
            .checked_mul(geometry.manifest_names)
            .and_then(|n| n.checked_add(self.names.len()))
            .ok_or(ContentError::Capacity)
    }
    /// How many whole chunks and manifest chunks it names: what a checkpoint naming it made
    /// durable ([`ColdStore::made_durable`]).
    pub fn named(&self, geometry: ColdGeometry) -> Result<(u64, u64), ContentError> {
        Ok((
            u64::try_from(self.chunks_named(geometry)?).map_err(|_| ContentError::Capacity)?,
            u64::try_from(self.manifests.len()).map_err(|_| ContentError::Capacity)?,
        ))
    }
    /// Whether the parts agree with each other and the stream's length.
    pub fn check(&self, geometry: ColdGeometry) -> Result<(), ContentError> {
        let geometry = geometry.valid()?;
        let whole = u64::try_from(self.chunks_named(geometry)?)
            .ok()
            .zip(u64::try_from(geometry.chunk_bytes).ok())
            .and_then(|(chunks, bytes)| chunks.checked_mul(bytes))
            .ok_or(ContentError::Capacity)?;
        let length = u64::try_from(self.open.len())
            .ok()
            .and_then(|open| whole.checked_add(open))
            .ok_or(ContentError::Capacity)?;
        if self.names.len() >= geometry.manifest_names
            || self.open.len() >= geometry.chunk_bytes
            || length != self.length
        {
            return Err(ContentError::Corrupt);
        }
        Ok(())
    }
    /// The whole chunks it names in order, those behind its manifest chunks read from `store`:
    /// every manifest chunk must be local.
    pub fn chunks(
        &self,
        geometry: ColdGeometry,
        store: &ColdReader,
    ) -> Result<Vec<ContentHash>, ContentError> {
        let mut chunks = Vec::new();
        chunks
            .try_reserve_exact(self.chunks_named(geometry)?)
            .map_err(|_| ContentError::Capacity)?;
        for manifest in &self.manifests {
            store.manifest_into(geometry, *manifest, &mut chunks)?;
        }
        chunks.extend_from_slice(&self.names);
        Ok(chunks)
    }
    /// The bytes of the stream it names from `offset`, `length` of them: from its open chunk and
    /// from the whole chunks `chunk` reads by their names (each checked against its name by the
    /// reader), those behind a manifest chunk found through it. What a backup and its verification
    /// read a stream from, without its writer.
    pub fn read(
        &self,
        geometry: ColdGeometry,
        offset: u64,
        length: usize,
        chunk: &dyn Fn(ContentHash, usize) -> Result<Vec<u8>, ContentError>,
    ) -> Result<Vec<u8>, ContentError> {
        let end = u64::try_from(length)
            .ok()
            .and_then(|length| offset.checked_add(length))
            .ok_or(ContentError::Capacity)?;
        if end > self.length {
            return Err(ContentError::Invalid);
        }
        let size = u64::try_from(geometry.chunk_bytes).map_err(|_| ContentError::Capacity)?;
        let whole =
            u64::try_from(self.chunks_named(geometry)?).map_err(|_| ContentError::Capacity)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length)
            .map_err(|_| ContentError::Capacity)?;
        let mut at = offset;
        // At most one read a chunk the range touches.
        while at < end {
            let index = at.checked_div(size).ok_or(ContentError::Capacity)?;
            let within = usize::try_from(at.checked_rem(size).ok_or(ContentError::Capacity)?)
                .map_err(|_| ContentError::Capacity)?;
            let take = usize::try_from(end.saturating_sub(at))
                .map_err(|_| ContentError::Capacity)?
                .min(geometry.chunk_bytes.saturating_sub(within));
            let stop = within.checked_add(take).ok_or(ContentError::Capacity)?;
            if index == whole {
                bytes.extend_from_slice(self.open.get(within..stop).ok_or(ContentError::Corrupt)?);
            } else {
                let per =
                    u64::try_from(geometry.manifest_names).map_err(|_| ContentError::Capacity)?;
                let manifest =
                    usize::try_from(index.checked_div(per).ok_or(ContentError::Capacity)?)
                        .map_err(|_| ContentError::Capacity)?;
                let position =
                    usize::try_from(index.checked_rem(per).ok_or(ContentError::Capacity)?)
                        .map_err(|_| ContentError::Capacity)?;
                let name = if manifest == self.manifests.len() {
                    *self.names.get(position).ok_or(ContentError::Invalid)?
                } else {
                    let listed = chunk(
                        *self.manifests.get(manifest).ok_or(ContentError::Invalid)?,
                        geometry.manifest_bytes()?,
                    )?;
                    let start = position.checked_mul(32).ok_or(ContentError::Capacity)?;
                    let name: [u8; 32] = listed
                        .get(start..start.checked_add(32).ok_or(ContentError::Capacity)?)
                        .and_then(|name| name.try_into().ok())
                        .ok_or(ContentError::Corrupt)?;
                    ContentHash(name)
                };
                let read = chunk(name, geometry.chunk_bytes)?;
                bytes.extend_from_slice(read.get(within..stop).ok_or(ContentError::Corrupt)?);
            }
            at = at
                .checked_add(u64::try_from(take).map_err(|_| ContentError::Capacity)?)
                .ok_or(ContentError::Capacity)?;
        }
        Ok(bytes)
    }
    /// What `store` lacks of what it names: its manifest chunks, then the chunks behind each
    /// manifest chunk `store` holds, then its last names. An install pulls these, and asks again
    /// once its manifest chunks arrived.
    pub fn missing(
        &self,
        geometry: ColdGeometry,
        store: &ColdReader,
    ) -> Result<Vec<ContentHash>, ContentError> {
        let mut missing = Vec::new();
        let mut push = |hash: ContentHash| -> Result<(), ContentError> {
            missing.try_reserve(1).map_err(|_| ContentError::Capacity)?;
            missing.push(hash);
            Ok(())
        };
        let mut behind = Vec::new();
        for manifest in &self.manifests {
            if !store.contains(*manifest) {
                push(*manifest)?;
                continue;
            }
            behind.clear();
            store.manifest_into(geometry, *manifest, &mut behind)?;
            for chunk in &behind {
                if !store.contains(*chunk) {
                    push(*chunk)?;
                }
            }
        }
        for chunk in &self.names {
            if !store.contains(*chunk) {
                push(*chunk)?;
            }
        }
        Ok(missing)
    }
}

/// The one writer of a session's cold directory.
pub struct ColdStore {
    root: PathBuf,
    disk: DiskBudget,
    geometry: ColdGeometry,
    failed: bool,
    length: u64,
    manifests: Vec<ContentHash>,
    names: Vec<ContentHash>,
    /// The open chunk (`open.<index>`, the index of the whole chunk it becomes), its length and
    /// the hash of what it holds so far.
    open: Option<File>,
    open_index: u64,
    open_len: usize,
    hasher: blake3::Hasher,
    /// The volume promised for the open chunk, kept once it is whole.
    promise: Option<DiskReservation>,
    /// Whole chunks and manifest chunks not yet durable (`.pending`), in the order they were
    /// cut, each with its place.
    pending: Vec<(Cut, ContentHash)>,
    /// What the names (`manifests`, `names`, `pending`) hold, charged before they grow.
    charge: Allocation,
    _writer_lock: focal_platform::FileLock,
}

/// What one append takes, promised before it is made ([`ColdStore::room`]): the volume of every
/// whole chunk and manifest chunk it cuts. It is for the next append only, of exactly its bytes.
#[derive(Debug)]
pub struct ColdRoom {
    at: u64,
    bytes: usize,
    volume: Option<DiskReservation>,
}

/// Where a cut chunk stands: the whole chunk at an index of the stream, or the manifest chunk at
/// an index of the manifest chunks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cut {
    Chunk(u64),
    Manifest(u64),
}

/// A session's cold streams as a checkpoint names them (31 M2, A2): how both are cut, the
/// retired families' batches, and the marks that find each batch by its record's sequence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColdStreams {
    pub geometry: ColdGeometry,
    pub data: ColdState,
    pub marks: ColdState,
}

/// A read-only view of a cold directory: what a reader of retired families and an installing
/// peer read, while its writer goes on.
#[derive(Clone)]
pub struct ColdReader {
    root: PathBuf,
}

fn open_path(root: &Path, index: u64) -> PathBuf {
    root.join(format!("open.{index}"))
}
fn chunk_path(root: &Path, hash: ContentHash) -> PathBuf {
    root.join(format!("{hash}.seed"))
}
fn pending_path(root: &Path, hash: ContentHash) -> PathBuf {
    root.join(format!("{hash}.pending"))
}
/// The bytes `manifests` manifest names, `names` names and `pending` pending cuts hold.
fn names_bytes(manifests: usize, names: usize, pending: usize) -> Result<usize, ContentError> {
    manifests
        .checked_add(names)
        .and_then(|n| n.checked_mul(size_of::<ContentHash>()))
        .zip(pending.checked_mul(size_of::<(Cut, ContentHash)>()))
        .and_then(|(names, pending)| names.checked_add(pending))
        .ok_or(ContentError::Capacity)
}
fn copy_of(names: &[ContentHash], room: usize) -> Result<Vec<ContentHash>, ContentError> {
    let mut copy = Vec::new();
    copy.try_reserve_exact(room.max(names.len()))
        .map_err(|_| ContentError::Capacity)?;
    copy.extend_from_slice(names);
    Ok(copy)
}

impl ColdStore {
    /// Open or create the directory, holding its writer lock, with the stream as `state` names it
    /// (a fresh stream when none): the open chunk written from its bytes, and what a crash left
    /// that no checkpoint names (an open chunk, pending chunks, a half-written file) removed,
    /// since replay cuts them again. The whole chunks it names are read as they are wanted, each
    /// checked by its hash.
    pub fn open(
        root: impl AsRef<Path>,
        disk: DiskBudget,
        memory: &MemoryBudget,
        geometry: ColdGeometry,
        state: Option<&ColdState>,
    ) -> Result<Self, ContentError> {
        let geometry = geometry.valid()?;
        let root = root.as_ref().to_path_buf();
        durable_directory(&root)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join("LOCK"))?;
        let lock = focal_platform::FileLock::exclusive(lock).map_err(|error| {
            if error.kind() == std::io::ErrorKind::WouldBlock {
                ContentError::Locked
            } else {
                ContentError::Io(error)
            }
        })?;
        let empty = ColdState::default();
        let state = state.unwrap_or(&empty);
        state.check(geometry)?;
        let charge = memory
            .reserve(
                BudgetKind::Index,
                BudgetLane::Completion,
                names_bytes(state.manifests.len(), state.names.len(), 0)?,
            )
            .map_err(|_| ContentError::Capacity)?
            .commit();
        for entry in fs::read_dir(&root)? {
            let entry = entry?;
            let name = entry.file_name();
            let unnamed = name.to_str().is_some_and(|name| {
                name.starts_with("open.")
                    || Path::new(name).extension().is_some_and(|extension| {
                        extension == "pending" || extension == "install" || extension == "writing"
                    })
            });
            if unnamed {
                fs::remove_file(entry.path())?;
            }
        }
        let open_index =
            u64::try_from(state.chunks_named(geometry)?).map_err(|_| ContentError::Capacity)?;
        let mut open = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(open_path(&root, open_index))?;
        open.write_all(&state.open)?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(&state.open);
        let promise = disk_reserve(
            &disk,
            &root,
            DiskKind::Archive,
            BudgetLane::Completion,
            u64::try_from(geometry.chunk_bytes).map_err(|_| ContentError::Capacity)?,
        )?;
        let mut store = Self {
            manifests: copy_of(&state.manifests, 0)?,
            names: copy_of(&state.names, 0)?,
            root,
            disk,
            geometry,
            failed: false,
            length: state.length,
            open: Some(open),
            open_index,
            open_len: state.open.len(),
            hasher,
            promise: Some(promise),
            pending: Vec::new(),
            charge,
            _writer_lock: lock,
        };
        // What the copies were given past their lengths, charged too.
        store.hold(0, 0, 0)?;
        Ok(store)
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn geometry(&self) -> ColdGeometry {
        self.geometry
    }
    pub fn reader(&self) -> ColdReader {
        ColdReader {
            root: self.root.clone(),
        }
    }
    /// The stream's length.
    pub fn length(&self) -> u64 {
        self.length
    }
    fn check(&self) -> Result<(), ContentError> {
        if self.failed {
            return Err(ContentError::Failed);
        }
        Ok(())
    }
    /// Promise what appending `bytes` more takes: the volume of every whole chunk the append cuts
    /// and of every manifest chunk, and the room to name them, charged before it is allocated.
    /// Refused (`Capacity`), nothing is promised and the stream is unchanged; granted, the next
    /// append of exactly those bytes fails only on IO.
    pub fn room(&mut self, bytes: usize) -> Result<ColdRoom, ContentError> {
        self.check()?;
        let geometry = self.geometry;
        let cuts = self
            .open_len
            .checked_add(bytes)
            .and_then(|end| end.checked_div(geometry.chunk_bytes))
            .ok_or(ContentError::Capacity)?;
        let manifests = self
            .names
            .len()
            .checked_add(cuts)
            .and_then(|named| named.checked_div(geometry.manifest_names))
            .ok_or(ContentError::Capacity)?;
        // The most names held at once: a manifest's worth, if one is cut.
        let names = if manifests > 0 {
            geometry.manifest_names
        } else {
            self.names.len().saturating_add(cuts)
        };
        let pending = self
            .pending
            .len()
            .checked_add(cuts)
            .and_then(|n| n.checked_add(manifests))
            .ok_or(ContentError::Capacity)?;
        let manifests_held = self
            .manifests
            .len()
            .checked_add(manifests)
            .ok_or(ContentError::Capacity)?;
        let volume = u64::try_from(cuts)
            .ok()
            .zip(u64::try_from(geometry.chunk_bytes).ok())
            .and_then(|(cuts, chunk)| cuts.checked_mul(chunk))
            .zip(
                u64::try_from(manifests)
                    .ok()
                    .zip(u64::try_from(geometry.manifest_bytes()?).ok())
                    .and_then(|(manifests, bytes)| manifests.checked_mul(bytes)),
            )
            .and_then(|(chunks, manifests)| chunks.checked_add(manifests))
            .ok_or(ContentError::Capacity)?;
        let volume = match volume {
            0 => None,
            volume => Some(disk_reserve(
                &self.disk,
                &self.root,
                DiskKind::Archive,
                BudgetLane::Completion,
                volume,
            )?),
        };
        // Refused here, the volume just promised is given back as it drops.
        self.hold(manifests_held, names, pending)?;
        Ok(ColdRoom {
            at: self.length,
            bytes,
            volume,
        })
    }
    /// Hold room for `manifests` manifest names, `names` names and `pending` pending cuts, each
    /// charged before it is allocated; what is held already is kept. Refused, the charge is what
    /// the names hold, as before.
    fn hold(&mut self, manifests: usize, names: usize, pending: usize) -> Result<(), ContentError> {
        let wanted = names_bytes(
            manifests.max(self.manifests.capacity()),
            names.max(self.names.capacity()),
            pending.max(self.pending.capacity()),
        )?;
        self.charge
            .grow(wanted.saturating_sub(self.charge.bytes()))
            .map_err(|_| ContentError::Capacity)?;
        let reserved = self
            .manifests
            .try_reserve_exact(manifests.saturating_sub(self.manifests.len()))
            .and_then(|()| {
                self.names
                    .try_reserve_exact(names.saturating_sub(self.names.len()))
            })
            .and_then(|()| {
                self.pending
                    .try_reserve_exact(pending.saturating_sub(self.pending.len()))
            });
        // The charge follows what is held: more if an allocator gave more than was asked, back
        // to it if the allocation was refused.
        let held = names_bytes(
            self.manifests.capacity(),
            self.names.capacity(),
            self.pending.capacity(),
        )?;
        let settled = match held.checked_sub(self.charge.bytes()) {
            Some(more) => self.charge.grow(more),
            None => self.charge.shrink_to(held),
        };
        reserved.map_err(|_| ContentError::Capacity)?;
        settled.map_err(|_| ContentError::Capacity)
    }
    /// Append `bytes` to the stream within the `room` promised for them, cutting a whole chunk at
    /// every chunk's worth of it. Nothing is synced. An IO failure fails the store until it is
    /// reopened, as a seed store's does: what it held is restored from the last checkpoint and
    /// replay. A room promised for other bytes, or before another append, is refused unused.
    pub fn append(&mut self, mut bytes: &[u8], room: ColdRoom) -> Result<(), ContentError> {
        self.check()?;
        if room.at != self.length || room.bytes != bytes.len() {
            return Err(ContentError::Invalid);
        }
        let mut volume = room.volume;
        let length = u64::try_from(bytes.len())
            .ok()
            .and_then(|added| self.length.checked_add(added))
            .ok_or(ContentError::Capacity)?;
        while !bytes.is_empty() {
            let room = self.geometry.chunk_bytes.saturating_sub(self.open_len);
            let (head, tail) = bytes
                .split_at_checked(room.min(bytes.len()))
                .ok_or(ContentError::Capacity)?;
            let written = match self.open.as_mut() {
                Some(open) => open.write_all(head).map_err(ContentError::from),
                None => Err(ContentError::Failed),
            };
            if let Err(error) = written {
                self.failed = true;
                return Err(error);
            }
            self.hasher.update(head);
            self.open_len = self
                .open_len
                .checked_add(head.len())
                .ok_or(ContentError::Capacity)?;
            bytes = tail;
            if self.open_len == self.geometry.chunk_bytes {
                let cut = self.cut(&mut volume);
                if cut.is_err() {
                    self.failed = true;
                }
                cut?;
            }
        }
        self.length = length;
        Ok(())
    }
    /// The open chunk is whole: closed, renamed under its hash as pending, named, and a fresh open
    /// chunk begun, its volume taken from `volume`; every manifest's worth of names is cut into a
    /// manifest chunk of its own, written whole before it is named, so a reader never meets part
    /// of one. The room was held before ([`Self::room`]): nothing here allocates a name.
    fn cut(&mut self, volume: &mut Option<DiskReservation>) -> Result<(), ContentError> {
        let mut take = |bytes: usize| -> Result<DiskReservation, ContentError> {
            volume
                .as_mut()
                .ok_or(ContentError::Failed)?
                .split_off(u64::try_from(bytes).map_err(|_| ContentError::Capacity)?)
                .map_err(|_| ContentError::Failed)
        };
        let hash = ContentHash(*self.hasher.finalize().as_bytes());
        // Closed before it is renamed: a file still open may not be renamed everywhere.
        drop(self.open.take());
        focal_platform::fs::atomic_replace(
            &open_path(&self.root, self.open_index),
            &pending_path(&self.root, hash),
        )?;
        if let Some(promise) = self.promise.take() {
            promise.commit();
        }
        if self.pending.len() == self.pending.capacity()
            || self.names.len() == self.names.capacity()
        {
            return Err(ContentError::Failed);
        }
        self.pending.push((Cut::Chunk(self.open_index), hash));
        self.names.push(hash);
        if self.names.len() == self.geometry.manifest_names {
            let record = take(self.geometry.manifest_bytes()?)?;
            let mut hasher = blake3::Hasher::new();
            let mut file_hash = |file: &mut File| -> Result<(), ContentError> {
                // A page of names at a time, from the stack.
                let mut page = [0u8; 4096];
                for group in self.names.chunks(page.len() / 32) {
                    let mut filled = 0usize;
                    for name in group {
                        let end = filled.checked_add(32).ok_or(ContentError::Capacity)?;
                        page.get_mut(filled..end)
                            .ok_or(ContentError::Capacity)?
                            .copy_from_slice(&name.0);
                        filled = end;
                    }
                    let bytes = page.get(..filled).ok_or(ContentError::Capacity)?;
                    hasher.update(bytes);
                    file.write_all(bytes)?;
                }
                Ok(())
            };
            let writing = self.root.join(format!("{}.writing", self.manifests.len()));
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&writing)?;
            file_hash(&mut file)?;
            drop(file);
            let hash = ContentHash(*hasher.finalize().as_bytes());
            focal_platform::fs::atomic_replace(&writing, &pending_path(&self.root, hash))?;
            record.commit();
            if self.manifests.len() == self.manifests.capacity()
                || self.pending.len() == self.pending.capacity()
            {
                return Err(ContentError::Failed);
            }
            let index = u64::try_from(self.manifests.len()).map_err(|_| ContentError::Capacity)?;
            self.manifests.push(hash);
            self.pending.push((Cut::Manifest(index), hash));
            self.names.clear();
        }
        self.open_index = self
            .open_index
            .checked_add(1)
            .ok_or(ContentError::Capacity)?;
        self.open = Some(
            OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .read(true)
                .open(open_path(&self.root, self.open_index))?,
        );
        self.open_len = 0;
        self.hasher.reset();
        self.promise = Some(take(self.geometry.chunk_bytes)?);
        Ok(())
    }
    /// The memory a capture of the stream holds ([`Self::capture`]): its open chunk's bytes and
    /// the names it copies.
    pub fn captured_bytes(&self) -> Result<usize, ContentError> {
        names_bytes(self.manifests.len(), self.names.len(), 0)?
            .checked_add(
                self.pending
                    .len()
                    .checked_mul(size_of::<ContentHash>())
                    .ok_or(ContentError::Capacity)?,
            )
            .and_then(|n| n.checked_add(self.open_len))
            .ok_or(ContentError::Capacity)
    }
    /// The stream as a checkpoint names it now, the open chunk's bytes read back through a
    /// handle of their own, and the chunks it names that are not durable yet: what the checkpoint
    /// makes durable ([`Self::make_durable`]) before it may be a recovery base.
    pub fn capture(&self) -> Result<(ColdState, Vec<ContentHash>), ContentError> {
        self.check()?;
        let mut open = Vec::new();
        open.try_reserve_exact(self.open_len)
            .map_err(|_| ContentError::Capacity)?;
        open.resize(self.open_len, 0);
        File::open(open_path(&self.root, self.open_index))?.read_exact(&mut open)?;
        let mut pending = Vec::new();
        pending
            .try_reserve_exact(self.pending.len())
            .map_err(|_| ContentError::Capacity)?;
        pending.extend(self.pending.iter().map(|(_, hash)| *hash));
        Ok((
            ColdState {
                length: self.length,
                manifests: copy_of(&self.manifests, 0)?,
                names: copy_of(&self.names, 0)?,
                open,
            },
            pending,
        ))
    }
    /// A checkpoint naming `chunks` whole chunks and `manifests` manifest chunks
    /// ([`ColdState::named`]) made them durable: those of them pending are pending no more.
    pub fn made_durable(&mut self, chunks: u64, manifests: u64) {
        self.pending.retain(|(cut, _)| match cut {
            Cut::Chunk(index) => *index >= chunks,
            Cut::Manifest(index) => *index >= manifests,
        });
    }
    /// The whole chunks and manifest chunks not yet durable.
    pub fn pending(&self) -> usize {
        self.pending.len()
    }
    /// The bytes of the stream from `offset`, `length` of them: from the whole chunks they lie in,
    /// each read and checked against its name, and the open chunk. A range past the stream is
    /// refused.
    pub fn read(&self, offset: u64, length: usize) -> Result<Vec<u8>, ContentError> {
        self.check()?;
        let end = u64::try_from(length)
            .ok()
            .and_then(|length| offset.checked_add(length))
            .ok_or(ContentError::Capacity)?;
        if end > self.length {
            return Err(ContentError::Invalid);
        }
        let chunk = u64::try_from(self.geometry.chunk_bytes).map_err(|_| ContentError::Capacity)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length)
            .map_err(|_| ContentError::Capacity)?;
        let reader = self.reader();
        let mut at = offset;
        // At most one read a chunk the range touches.
        while at < end {
            let index = at.checked_div(chunk).ok_or(ContentError::Capacity)?;
            let within = usize::try_from(at.checked_rem(chunk).ok_or(ContentError::Capacity)?)
                .map_err(|_| ContentError::Capacity)?;
            let take = usize::try_from(end.saturating_sub(at))
                .map_err(|_| ContentError::Capacity)?
                .min(self.geometry.chunk_bytes.saturating_sub(within));
            if index == self.open_index {
                let start = bytes.len();
                let stop = start.checked_add(take).ok_or(ContentError::Capacity)?;
                bytes.resize(stop, 0);
                let mut file = File::open(open_path(&self.root, index))?;
                file.seek(SeekFrom::Start(
                    u64::try_from(within).map_err(|_| ContentError::Capacity)?,
                ))?;
                file.read_exact(bytes.get_mut(start..stop).ok_or(ContentError::Capacity)?)?;
            } else {
                let name = self.chunk_name(&reader, index)?;
                let whole = reader.read(name, self.geometry.chunk_bytes)?;
                let end_within = within.checked_add(take).ok_or(ContentError::Capacity)?;
                bytes
                    .extend_from_slice(whole.get(within..end_within).ok_or(ContentError::Corrupt)?);
            }
            at = at
                .checked_add(u64::try_from(take).map_err(|_| ContentError::Capacity)?)
                .ok_or(ContentError::Capacity)?;
        }
        Ok(bytes)
    }
    /// The name of the whole chunk at `index`: among the last names, or in its manifest chunk.
    fn chunk_name(&self, reader: &ColdReader, index: u64) -> Result<ContentHash, ContentError> {
        let per =
            u64::try_from(self.geometry.manifest_names).map_err(|_| ContentError::Capacity)?;
        let manifest = usize::try_from(index.checked_div(per).ok_or(ContentError::Capacity)?)
            .map_err(|_| ContentError::Capacity)?;
        let within = usize::try_from(index.checked_rem(per).ok_or(ContentError::Capacity)?)
            .map_err(|_| ContentError::Capacity)?;
        if manifest == self.manifests.len() {
            return self.names.get(within).copied().ok_or(ContentError::Invalid);
        }
        let mut names = Vec::new();
        reader.manifest_into(
            self.geometry,
            *self.manifests.get(manifest).ok_or(ContentError::Invalid)?,
            &mut names,
        )?;
        names.get(within).copied().ok_or(ContentError::Corrupt)
    }
    /// Make the chunks `pending` names durable in the directory at `root`, on whichever thread
    /// holds a checkpoint's encoding: every one synced, then renamed under its hash, then the
    /// directory synced once. A chunk already renamed by an earlier commit that failed after it
    /// counts as done; one that is nowhere fails the commit.
    pub fn make_durable(root: &Path, pending: &[ContentHash]) -> Result<(), ContentError> {
        if pending.is_empty() {
            return Ok(());
        }
        for hash in pending {
            match File::open(pending_path(root, *hash)) {
                Ok(file) => file.sync_all()?,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    if !chunk_path(root, *hash).is_file() {
                        return Err(ContentError::Failed);
                    }
                }
                Err(error) => return Err(error.into()),
            }
        }
        for hash in pending {
            let from = pending_path(root, *hash);
            if from.is_file() {
                focal_platform::fs::atomic_replace(&from, &chunk_path(root, *hash))?;
            }
        }
        sync_directory(root)?;
        Ok(())
    }
    /// Take a chunk a peer sent for an install, verified against its hash, and make it durable.
    pub fn install_as(&mut self, hash: ContentHash, bytes: &[u8]) -> Result<(), ContentError> {
        self.check()?;
        if bytes.is_empty()
            || bytes.len() > SEED_CHUNK_BYTES
            || ContentHash(*blake3::hash(bytes).as_bytes()) != hash
        {
            return Err(ContentError::Corrupt);
        }
        let record = disk_reserve(
            &self.disk,
            &self.root,
            DiskKind::Archive,
            BudgetLane::Completion,
            u64::try_from(bytes.len()).map_err(|_| ContentError::Capacity)?,
        )?;
        if let Err(error) = install_verified_chunk(&chunk_path(&self.root, hash), bytes, hash) {
            if matches!(error, ContentError::Io(_)) {
                self.failed = true;
            }
            return Err(error);
        }
        record.commit();
        Ok(())
    }
}

/// Take a chunk a peer sent for an install into the cold directory at `root`, verified against
/// its hash and made durable under it. It needs no writer: a chunk is a file named by its hash,
/// so an install beside a writer, or beside another install of the same chunk, writes the same
/// bytes.
pub fn install_cold_chunk(
    root: &Path,
    disk: &DiskBudget,
    hash: ContentHash,
    bytes: &[u8],
) -> Result<(), ContentError> {
    if bytes.is_empty()
        || bytes.len() > SEED_CHUNK_BYTES
        || ContentHash(*blake3::hash(bytes).as_bytes()) != hash
    {
        return Err(ContentError::Corrupt);
    }
    durable_directory(root)?;
    let record = disk_reserve(
        disk,
        root,
        DiskKind::Archive,
        BudgetLane::Completion,
        u64::try_from(bytes.len()).map_err(|_| ContentError::Capacity)?,
    )?;
    install_verified_chunk(&chunk_path(root, hash), bytes, hash)?;
    record.commit();
    Ok(())
}

impl ColdReader {
    pub fn open(root: impl AsRef<Path>) -> Result<Self, ContentError> {
        let root = root.as_ref().to_path_buf();
        if !root.is_dir() {
            return Err(ContentError::Invalid);
        }
        Ok(Self { root })
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
    /// Whether a whole chunk or manifest chunk is here, durable or pending.
    pub fn contains(&self, hash: ContentHash) -> bool {
        chunk_path(&self.root, hash).is_file() || pending_path(&self.root, hash).is_file()
    }
    /// A whole chunk or manifest chunk, checked against its hash: durable, or pending (a
    /// checkpoint making it durable meanwhile renames it, so it is looked for once more).
    pub fn read(&self, hash: ContentHash, max_bytes: usize) -> Result<Vec<u8>, ContentError> {
        let limit = max_bytes.min(SEED_CHUNK_BYTES);
        let durable = chunk_path(&self.root, hash);
        let absent = |result: &Result<Vec<u8>, ContentError>| matches!(result, Err(ContentError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound);
        let mut bytes = read_bounded(&durable, limit);
        if absent(&bytes) {
            bytes = read_bounded(&pending_path(&self.root, hash), limit);
            if absent(&bytes) {
                bytes = read_bounded(&durable, limit);
            }
        }
        let bytes = bytes?;
        if ContentHash(*blake3::hash(&bytes).as_bytes()) != hash {
            return Err(ContentError::Corrupt);
        }
        Ok(bytes)
    }
    /// The chunk names a manifest chunk holds, appended to `into`.
    fn manifest_into(
        &self,
        geometry: ColdGeometry,
        manifest: ContentHash,
        into: &mut Vec<ContentHash>,
    ) -> Result<(), ContentError> {
        let size = geometry.manifest_bytes()?;
        let bytes = self.read(manifest, size)?;
        if bytes.len() != size {
            return Err(ContentError::Corrupt);
        }
        into.try_reserve(geometry.manifest_names)
            .map_err(|_| ContentError::Capacity)?;
        let (names, rest) = bytes.as_chunks::<32>();
        if !rest.is_empty() {
            return Err(ContentError::Corrupt);
        }
        into.extend(names.iter().map(|name| ContentHash(*name)));
        Ok(())
    }
}

#[cfg(test)]
#[path = "cold_tests.rs"]
mod tests;
