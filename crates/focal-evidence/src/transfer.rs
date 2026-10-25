//! Exact content-tree transfer. A receiver preserves the sender's manifest and
//! chunk boundaries, independent of its preferred upload chunk size.
//!
//! A chunk received in parts is staged beside the objects until it is whole (29 §12.7): in the
//! clear, a file named by the chunk's hash; where the store seals, a record file sealed as it is
//! written ([`record_file`]), kept for the chunk (its key derived with the chunk's hash, so a part
//! under another chunk's name opens to nothing) and named by its domain's keyed name of the hash. A
//! part in the clear goes on in the clear until the store's conversion seals it; a new part is
//! written in the store's form.
use super::record_file::{self, PART_MAGIC, RecordWriter, Verify};
use super::*;

/// A new part of chunk `hash` at `path` under `key`, its record file made and synced: a file ID of
/// its own, its key kept for the chunk. `most` is the chunk's length.
pub(super) fn create_part(
    key: &sealing::DomainKey,
    path: &Path,
    hash: ContentHash,
    most: usize,
) -> Result<RecordWriter, ContentError> {
    let file = hyper_seal::keys::KeyId::random()
        .map_err(|_| {
            ContentError::Io(std::io::Error::other(
                "the operating system's random source failed",
            ))
        })?
        .0;
    let writer = RecordWriter::create(path, PART_MAGIC, file, key.part_key(&file, &hash.0)?, most)?;
    writer.sync()?;
    Ok(writer)
}

/// A chunk's part as it stands: in the clear (its bytes, its file's length), or sealed (its
/// writer, read through to its end).
enum StagedPart {
    Clear {
        path: PathBuf,
        held: u64,
    },
    Sealed {
        path: PathBuf,
        writer: Box<RecordWriter>,
    },
}

impl StagedPart {
    fn held(&self) -> u64 {
        match self {
            Self::Clear { held, .. } => *held,
            Self::Sealed { writer, .. } => writer.plain(),
        }
    }
}

/// Validated immutable transfer description. A host owns and budgets this value
/// while transferring; cloning is deliberately not part of the interface.
pub struct TransferManifest {
    reference: ContentRef,
    encoded: Vec<u8>,
    manifest: Manifest,
}
impl TransferManifest {
    pub fn reference(&self) -> &ContentRef {
        &self.reference
    }
    pub fn encoded(&self) -> &[u8] {
        &self.encoded
    }
    pub fn chunks(&self) -> usize {
        self.manifest.chunks.len()
    }
    pub fn stream_digest(&self) -> ContentHash {
        self.manifest.stream_digest
    }
    /// The hash and length of one chunk of the tree.
    pub fn chunk(&self, index: usize) -> Result<(ContentHash, u32), ContentError> {
        self.manifest
            .chunks
            .get(index)
            .map(|chunk| (chunk.hash, chunk.length))
            .ok_or(ContentError::Invalid)
    }
    pub fn chunk_length(&self, index: usize) -> Result<usize, ContentError> {
        usize::try_from(
            self.manifest
                .chunks
                .get(index)
                .ok_or(ContentError::Invalid)?
                .length,
        )
        .map_err(|_| ContentError::Capacity)
    }
    /// Retained heap and value storage; the host reserves this before retaining
    /// the descriptor in a transfer job. Network decode has a separate frame cap.
    pub fn resident_bytes(&self) -> Result<usize, ContentError> {
        self.manifest
            .chunks
            .capacity()
            .checked_mul(std::mem::size_of::<Chunk>())
            .and_then(|n| n.checked_add(self.encoded.capacity()))
            .and_then(|n| n.checked_add(std::mem::size_of::<Self>()))
            .ok_or(ContentError::Capacity)
    }
    /// The most a manifest of `encoded` bytes holds read and decoded: its
    /// encoding, the chunk list it names, and the descriptor
    /// ([`Self::resident_bytes`] is never more).
    pub fn resident_for(encoded: usize) -> Result<usize, ContentError> {
        chunk_list_bound(encoded)
            .and_then(|n| n.checked_add(encoded))
            .and_then(|n| n.checked_add(std::mem::size_of::<Self>()))
            .ok_or(ContentError::Capacity)
    }
}

impl ContentStore {
    /// Exports the exact authenticated tree rather than rechunking its bytes.
    pub fn export_manifest(
        &self,
        reference: &ContentRef,
    ) -> Result<TransferManifest, ContentError> {
        self.export_manifest_within(reference, MAX_TRANSFER_MANIFEST_BYTES)
    }
    /// [`Self::export_manifest`] of a manifest at most `encoded` bytes (as
    /// [`Self::manifest_length`] found it, and its holder charged
    /// [`TransferManifest::resident_for`] it): a longer one is refused before
    /// it is read.
    pub fn export_manifest_within(
        &self,
        reference: &ContentRef,
        encoded: usize,
    ) -> Result<TransferManifest, ContentError> {
        self.check()?;
        let encoded = manifest_plain(
            &self.root,
            &self.keys,
            reference.domain,
            reference.root,
            encoded,
        )?;
        self.prepare_import(reference.clone(), encoded)
    }

    /// Checks the entire manifest before accepting any transfer chunk. The
    /// transport authorizes the session/domain before invoking this storage API.
    pub fn prepare_import(
        &self,
        reference: ContentRef,
        encoded: Vec<u8>,
    ) -> Result<TransferManifest, ContentError> {
        self.check()?;
        if encoded.len() > MAX_TRANSFER_MANIFEST_BYTES
            || reference.length > MAX_TRANSFER_CONTENT_BYTES
        {
            return Err(ContentError::Capacity);
        }
        if !encoded.starts_with(MANIFEST_MAGIC)
            || ContentHash(*blake3::hash(&encoded).as_bytes()) != reference.root
        {
            return Err(ContentError::Corrupt);
        }
        let manifest = decode_manifest(
            encoded
                .get(MANIFEST_MAGIC.len()..)
                .ok_or(ContentError::Corrupt)?,
        )?;
        let total = manifest
            .chunks
            .iter()
            .try_fold(0u64, |sum, chunk| sum.checked_add(u64::from(chunk.length)))
            .ok_or(ContentError::Corrupt)?;
        if manifest.schema != 1
            || manifest.domain != reference.domain
            || manifest.class != reference.class
            || manifest.length != reference.length
            || total != reference.length
            || manifest
                .chunks
                .iter()
                .any(|chunk| chunk.length == 0 || chunk.length as usize > MAX_TRANSFER_CHUNK_BYTES)
        {
            return Err(ContentError::Corrupt);
        }
        Ok(TransferManifest {
            reference,
            encoded,
            manifest,
        })
    }

    /// Reads one authenticated chunk for export, or checks a receiver's already
    /// installed chunk during resume. Missing/corrupt chunks cannot be reported
    /// as durable copies.
    pub fn read_transfer_chunk(
        &self,
        transfer: &TransferManifest,
        index: usize,
    ) -> Result<Vec<u8>, ContentError> {
        self.check()?;
        let chunk = transfer
            .manifest
            .chunks
            .get(index)
            .ok_or(ContentError::Invalid)?;
        if chunk.length as usize > MAX_TRANSFER_CHUNK_BYTES {
            return Err(ContentError::Capacity);
        }
        let domain = transfer.reference.domain;
        let directory = domain_directory(&self.root, domain);
        let key = self.keys.domain(&directory, domain, false)?;
        let mut bytes = zeroed_buffer(chunk.length as usize)?;
        verify_chunk_into(
            &directory,
            &key,
            chunk.hash,
            chunk.length as usize,
            &mut bytes,
        )?;
        Ok(bytes)
    }

    /// Each accepted chunk is file- and directory-synced. A lost response can be
    /// retried exactly; an incomplete transfer does not install a manifest.
    pub fn import_chunk(
        &mut self,
        transfer: &TransferManifest,
        index: usize,
        bytes: &[u8],
    ) -> Result<(), ContentError> {
        self.check()?;
        let result = (|| {
            let chunk = transfer
                .manifest
                .chunks
                .get(index)
                .ok_or(ContentError::Invalid)?;
            if bytes.len() > MAX_TRANSFER_CHUNK_BYTES
                || transfer.reference.length > MAX_TRANSFER_CONTENT_BYTES
            {
                return Err(ContentError::Capacity);
            }
            if bytes.len() != chunk.length as usize
                || ContentHash(*blake3::hash(bytes).as_bytes()) != chunk.hash
            {
                return Err(ContentError::Corrupt);
            }
            let domain = transfer.reference.domain;
            let directory = domain_directory(&self.root, domain);
            let key = self.keys.domain(&directory, domain, false)?;
            // A verified duplicate — an exact retry, or a resumed transfer's
            // chunk already held, in either form — adds no byte to the volume:
            // it needs no promise and lowers no estimate (the audit's F59).
            if already_held(
                &key,
                &directory,
                chunk.hash,
                sealing::Kind::Chunk,
                bytes.len(),
            )? {
                return Ok(());
            }
            // Custody transfers are fleet work: they draw on the completion
            // lane so a tenant's uploads cannot starve replication. The chunk
            // is promised its file's bytes as the writer installs it, and the
            // domain its key record where the writer seals and it has none
            // yet (29 §11).
            let copy = disk_reserve(
                &self.disk,
                &self.root,
                DiskKind::Content,
                focal_memory::BudgetLane::Completion,
                key.file_bytes(bytes.len())?
                    .checked_add(key.record_bytes())
                    .ok_or(ContentError::Capacity)?,
            )?;
            durable_directory(&directory)?;
            let key = self.keys.writable(&directory, domain, key)?;
            key.install(
                &key.path(&directory, chunk.hash, sealing::Kind::Chunk)?,
                bytes,
            )?;
            copy.commit();
            Ok(())
        })();
        self.mark_failure(&result);
        result
    }

    /// Whether chunk `index` of `transfer` is installed under its hash: the
    /// object's chunk this store already holds, as a content-addressed chunk of
    /// another object may be. What is installed is verified as it is read.
    pub fn chunk_installed(
        &self,
        transfer: &TransferManifest,
        index: usize,
    ) -> Result<bool, ContentError> {
        self.check()?;
        let chunk = transfer
            .manifest
            .chunks
            .get(index)
            .ok_or(ContentError::Invalid)?;
        let domain = transfer.reference.domain;
        let directory = domain_directory(&self.root, domain);
        self.keys.domain(&directory, domain, false)?.exists(
            &directory,
            chunk.hash,
            sealing::Kind::Chunk,
        )
    }
    /// How much of chunk `index` is staged in parts: the offset the next
    /// part is taken at. Zero where none is, or the chunk is installed.
    pub fn staged_chunk_bytes(
        &self,
        transfer: &TransferManifest,
        index: usize,
    ) -> Result<u64, ContentError> {
        self.check()?;
        let chunk = transfer
            .manifest
            .chunks
            .get(index)
            .ok_or(ContentError::Invalid)?;
        Ok(self
            .staged_part(transfer, chunk.hash, chunk.length)?
            .map_or(0, |part| part.held().min(u64::from(chunk.length))))
    }
    /// Take a part of chunk `index` that begins at `offset`: a chunk that
    /// would take its path longer than a transfer's lease to cross goes in
    /// parts (the audit's F49). Parts go in order and are held in a
    /// staging file beside the objects until the chunk is whole; a part
    /// that is wholly held already is an exact retry and takes nothing, one
    /// that overlaps what is held adds what it brings past it (a sender
    /// that lost a reply, or begins the chunk again, sends from where it
    /// was); one that begins past what is held is a gap and is refused.
    /// When the
    /// last byte is held the chunk is verified against the manifest's hash
    /// and installed as `import_chunk` installs one, under the same name;
    /// a chunk that does not verify is discarded whole. The staged bytes
    /// are promised to the volume as they are taken, part by part, and the
    /// installed chunk is the same bytes moved: nothing is promised twice.
    /// Returns how much of the chunk is held after the part, the chunk's
    /// length once it is installed.
    pub fn import_chunk_part(
        &mut self,
        transfer: &TransferManifest,
        index: usize,
        offset: u64,
        bytes: &[u8],
    ) -> Result<u64, ContentError> {
        self.check()?;
        let result = (|| {
            let chunk = transfer
                .manifest
                .chunks
                .get(index)
                .ok_or(ContentError::Invalid)?;
            let length = u64::from(chunk.length);
            let part = u64::try_from(bytes.len()).map_err(|_| ContentError::Capacity)?;
            if bytes.is_empty()
                || offset.checked_add(part).is_none_or(|end| end > length)
                || transfer.reference.length > MAX_TRANSFER_CONTENT_BYTES
            {
                return Err(ContentError::Invalid);
            }
            let domain = transfer.reference.domain;
            let directory = domain_directory(&self.root, domain);
            if self.chunk_installed(transfer, index)?
                && self.read_transfer_chunk(transfer, index).is_ok()
            {
                // Held whole already (F59): the part adds nothing.
                return Ok(length);
            }
            let most = usize::try_from(chunk.length).map_err(|_| ContentError::Capacity)?;
            // The part as it stands, in its form; a new one in the store's.
            let mut staged_part = match self.staged_part(transfer, chunk.hash, chunk.length) {
                Ok(staged_part) => staged_part,
                Err(ContentError::Corrupt) => {
                    // A sealed part that does not open as a crash leaves one: discarded, the
                    // chunk taken again from its start.
                    self.discard_chunk_part(transfer, chunk.hash)?;
                    return Err(ContentError::Corrupt);
                }
                Err(error) => return Err(error),
            };
            let staged = staged_part.as_ref().map_or(0, StagedPart::held);
            let end = offset.saturating_add(part);
            if end <= staged {
                // An exact retry of a part already held.
                return Ok(staged);
            }
            if offset > staged {
                return Err(ContentError::Invalid);
            }
            // What the part brings past what is held.
            let skip = usize::try_from(staged.saturating_sub(offset))
                .map_err(|_| ContentError::Capacity)?;
            let fresh = bytes.get(skip..).ok_or(ContentError::Invalid)?;
            durable_directory(&directory)?;
            if staged_part.is_none() {
                let key = self.keys.domain(&directory, domain, false)?;
                staged_part = Some(if self.rest.seals() {
                    let header = disk_reserve(
                        &self.disk,
                        &self.root,
                        DiskKind::Content,
                        focal_memory::BudgetLane::Completion,
                        u64::try_from(record_file::PART_HEADER)
                            .map_err(|_| ContentError::Capacity)?
                            .checked_add(key.record_bytes())
                            .ok_or(ContentError::Capacity)?,
                    )?;
                    let key = self.keys.writable(&directory, domain, key)?;
                    let path = key.path(&directory, chunk.hash, sealing::Kind::Part)?;
                    let writer = create_part(&key, &path, chunk.hash, most)?;
                    header.commit();
                    StagedPart::Sealed {
                        path,
                        writer: Box::new(writer),
                    }
                } else {
                    StagedPart::Clear {
                        path: directory.join(format!("{}.part", chunk.hash)),
                        held: 0,
                    }
                });
            }
            let Some(staged_part) = staged_part.as_mut() else {
                return Err(ContentError::Invalid);
            };
            let fresh_bytes = u64::try_from(fresh.len()).map_err(|_| ContentError::Capacity)?;
            match staged_part {
                StagedPart::Clear { path, .. } => {
                    let promise = disk_reserve(
                        &self.disk,
                        &self.root,
                        DiskKind::Content,
                        focal_memory::BudgetLane::Completion,
                        fresh_bytes,
                    )?;
                    {
                        let mut file = OpenOptions::new().create(true).append(true).open(&*path)?;
                        file.write_all(fresh)?;
                        file.sync_all()?;
                    }
                    promise.commit();
                    // A part in the clear grew while the store converts: its count follows it.
                    self.conversion_part(domain, convert::StagingChange::Grew(fresh_bytes))?;
                }
                StagedPart::Sealed { writer, .. } => {
                    let promise = disk_reserve(
                        &self.disk,
                        &self.root,
                        DiskKind::Content,
                        focal_memory::BudgetLane::Completion,
                        record_file::records_bytes(fresh.len())?,
                    )?;
                    writer.append(fresh)?;
                    writer.sync()?;
                    promise.commit();
                }
            }
            if end < length {
                return Ok(end);
            }
            // Whole: every byte opened, verified, then the staged bytes become the chunk.
            let (held, path, clear) = match staged_part {
                StagedPart::Clear { path, .. } => (read_bounded(path, most)?, path.clone(), true),
                StagedPart::Sealed { path, writer } => {
                    let mut held = Vec::new();
                    held.try_reserve_exact(most)
                        .map_err(|_| ContentError::Capacity)?;
                    held.resize(most, 0);
                    let opened = writer.read(path, 0, &mut held);
                    if !matches!(opened, Ok(())) {
                        held.clear();
                    }
                    (held, path.clone(), false)
                }
            };
            if held.len() != chunk.length as usize
                || ContentHash(*blake3::hash(&held).as_bytes()) != chunk.hash
            {
                self.discard_chunk_part(transfer, chunk.hash)?;
                return Err(ContentError::Corrupt);
            }
            // Installed in its place as the writer installs, sealed under its domain's key from the
            // fence on (29 §11), promised its file's bytes and the domain its key record where the
            // writer seals and it has none yet; the staged part then goes.
            let key = self.keys.domain(&directory, domain, false)?;
            let installed = disk_reserve(
                &self.disk,
                &self.root,
                DiskKind::Content,
                focal_memory::BudgetLane::Completion,
                key.file_bytes(held.len())?
                    .checked_add(key.record_bytes())
                    .ok_or(ContentError::Capacity)?,
            )?;
            let key = self.keys.writable(&directory, domain, key)?;
            key.install(
                &key.path(&directory, chunk.hash, sealing::Kind::Chunk)?,
                &held,
            )?;
            installed.commit();
            std::fs::remove_file(&path)?;
            if clear {
                // A part in the clear went, whole and installed, while the store converts.
                self.conversion_part(
                    domain,
                    convert::StagingChange::Gone(u64::try_from(held.len()).unwrap_or(u64::MAX)),
                )?;
            }
            // A sealed copy a crash in the conversion left beside a part in the clear goes too.
            for (other, _) in self.part_places(transfer, chunk.hash)? {
                match std::fs::remove_file(&other) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
            }
            sync_directory(&directory)?;
            Ok(length)
        })();
        self.mark_failure(&result);
        result
    }
    /// Discard the parts of every chunk of `transfer` still staged: a
    /// transfer that expired, was cancelled or failed holds no more of the
    /// volume. Chunks installed whole stay, as `import_chunk`'s do.
    pub fn discard_chunk_parts(&mut self, transfer: &TransferManifest) -> Result<(), ContentError> {
        self.check()?;
        let result = (|| {
            let directory = self
                .root
                .join("objects")
                .join(hex(&transfer.reference.domain.0));
            let mut removed = false;
            for chunk in &transfer.manifest.chunks {
                removed |= self.remove_part(transfer, chunk.hash)?;
            }
            if removed {
                sync_directory(&directory)?;
            }
            Ok(())
        })();
        self.mark_failure(&result);
        result
    }
    /// Where chunk `hash`'s part of `transfer` may lie, each form's place with whether it seals: in
    /// the clear while a file may lie in the clear, under its domain's keyed name once the domain's
    /// key stands.
    fn part_places(
        &self,
        transfer: &TransferManifest,
        hash: ContentHash,
    ) -> Result<Vec<(PathBuf, bool)>, ContentError> {
        let domain = transfer.reference.domain;
        let directory = domain_directory(&self.root, domain);
        let mut places = Vec::new();
        places
            .try_reserve_exact(2)
            .map_err(|_| ContentError::Capacity)?;
        if self.rest.clear_allowed() {
            places.push((directory.join(format!("{hash}.part")), false));
        }
        let key = self.keys.domain(&directory, domain, false)?;
        if self.rest.seals() && key.record_bytes() == 0 {
            places.push((key.path(&directory, hash, sealing::Kind::Part)?, true));
        }
        Ok(places)
    }

    /// Chunk `hash`'s part of `transfer` as it stands, the first form it lies in: in the clear first
    /// (a sealed copy beside it a crash in the conversion left), then sealed, read through by its
    /// length blocks ([`Verify::Lengths`]). None where no part stands.
    fn staged_part(
        &self,
        transfer: &TransferManifest,
        hash: ContentHash,
        length: u32,
    ) -> Result<Option<StagedPart>, ContentError> {
        let domain = transfer.reference.domain;
        let directory = domain_directory(&self.root, domain);
        let most = usize::try_from(length).map_err(|_| ContentError::Capacity)?;
        for (path, sealed) in self.part_places(transfer, hash)? {
            if !sealed {
                match std::fs::metadata(&path) {
                    Ok(metadata) => {
                        return Ok(Some(StagedPart::Clear {
                            path,
                            held: metadata.len(),
                        }));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(error) => return Err(error.into()),
                }
            }
            if !path.exists() {
                continue;
            }
            let key = self.keys.domain(&directory, domain, false)?;
            let writer = RecordWriter::resume(
                &path,
                PART_MAGIC,
                &|file: &[u8; 16]| key.part_key(file, &hash.0),
                most,
                record_file::records_bytes(most)?,
                Verify::Lengths,
            )?;
            return Ok(Some(StagedPart::Sealed {
                path,
                writer: Box::new(writer),
            }));
        }
        Ok(None)
    }

    /// Chunk `hash`'s part of `transfer` removed in whichever form it lies: whether any was. A part in
    /// the clear that goes while the store converts leaves its count.
    fn remove_part(
        &mut self,
        transfer: &TransferManifest,
        hash: ContentHash,
    ) -> Result<bool, ContentError> {
        let mut removed = false;
        for (path, sealed) in self.part_places(transfer, hash)? {
            let bytes = match std::fs::metadata(&path) {
                Ok(metadata) => metadata.len(),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            match std::fs::remove_file(&path) {
                Ok(()) => removed = true,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            }
            if !sealed {
                self.conversion_part(
                    transfer.reference.domain,
                    convert::StagingChange::Gone(bytes),
                )?;
            }
        }
        Ok(removed)
    }

    /// Chunk `hash`'s part of `transfer` discarded: removed in either form, its directory synced.
    fn discard_chunk_part(
        &mut self,
        transfer: &TransferManifest,
        hash: ContentHash,
    ) -> Result<(), ContentError> {
        if self.remove_part(transfer, hash)? {
            sync_directory(&domain_directory(&self.root, transfer.reference.domain))?;
        }
        Ok(())
    }

    /// Verifies every chunk and the whole stream before publishing the original
    /// manifest. Success proves local durable custody of exactly this root.
    /// At once: [`Self::advance_completion`] to its end, for a caller that
    /// holds the store alone (a tool, a backup's restore); a node's content
    /// owner completes a transfer a chunk a call.
    pub fn complete_import(
        &mut self,
        transfer: &TransferManifest,
    ) -> Result<ContentRef, ContentError> {
        let mut completion = self.begin_completion(transfer)?;
        let largest = transfer
            .manifest
            .chunks
            .iter()
            .map(|chunk| chunk.length as usize)
            .max()
            .unwrap_or(0);
        let mut scratch = zeroed_buffer(largest)?;
        // A slice a chunk, and the manifest's.
        for _ in 0..=transfer.manifest.chunks.len() {
            if let Some(reference) =
                self.advance_completion(transfer, &mut completion, &mut scratch)?
            {
                return Ok(reference);
            }
        }
        Err(ContentError::Failed)
    }
    /// Begin a transfer's completion in slices ([`ImportCompletion`]):
    /// nothing read yet.
    pub fn begin_completion(
        &self,
        transfer: &TransferManifest,
    ) -> Result<ImportCompletion, ContentError> {
        self.check()?;
        if transfer.encoded.len() > MAX_TRANSFER_MANIFEST_BYTES
            || transfer.reference.length > MAX_TRANSFER_CONTENT_BYTES
        {
            return Err(ContentError::Capacity);
        }
        Ok(ImportCompletion {
            root: transfer.reference.root,
            next: 0,
            received: 0,
            whole: blake3::Hasher::new(),
        })
    }
    /// One slice of a transfer's completion (the audit's F51): the next
    /// chunk read back into `scratch` and verified against its name, its
    /// bytes hashed into the stream; past the last, the stream checked
    /// against the manifest's digest and the manifest installed, and its
    /// reference answered. A chunk not held is `Incomplete`; nothing is
    /// installed before every chunk and the stream verify.
    pub fn advance_completion(
        &mut self,
        transfer: &TransferManifest,
        completion: &mut ImportCompletion,
        scratch: &mut [u8],
    ) -> Result<Option<ContentRef>, ContentError> {
        self.check()?;
        let result = self.advance_completion_inner(transfer, completion, scratch);
        self.mark_failure(&result);
        result
    }
    fn advance_completion_inner(
        &mut self,
        transfer: &TransferManifest,
        completion: &mut ImportCompletion,
        scratch: &mut [u8],
    ) -> Result<Option<ContentRef>, ContentError> {
        if completion.root != transfer.reference.root {
            return Err(ContentError::Invalid);
        }
        let domain = transfer.reference.domain;
        let directory = domain_directory(&self.root, domain);
        // A domain this store holds nothing of holds none of the transfer's chunks.
        let key = self.keys.domain(&directory, domain, false)?;
        if key.holds_nothing() {
            return Err(ContentError::Incomplete {
                received: completion.received,
                expected: transfer.reference.length,
            });
        }
        if let Some(chunk) = transfer.manifest.chunks.get(completion.next) {
            let bytes = match verify_chunk_into(
                &directory,
                &key,
                chunk.hash,
                chunk.length as usize,
                scratch,
            ) {
                Err(ContentError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Err(ContentError::Incomplete {
                        received: completion.received,
                        expected: transfer.reference.length,
                    });
                }
                result => result?,
            };
            completion.whole.update(bytes);
            completion.received = completion
                .received
                .checked_add(u64::from(chunk.length))
                .ok_or(ContentError::Corrupt)?;
            completion.next = completion
                .next
                .checked_add(1)
                .ok_or(ContentError::Capacity)?;
            return Ok(None);
        }
        if ContentHash(*completion.whole.finalize().as_bytes()) != transfer.manifest.stream_digest {
            return Err(ContentError::Corrupt);
        }
        // A repeated completion finds its manifest installed, in either form:
        // no promise, no byte.
        if already_held(
            &key,
            &directory,
            transfer.reference.root,
            sealing::Kind::Manifest,
            transfer.encoded.len(),
        )? {
            return Ok(Some(transfer.reference.clone()));
        }
        let manifest = disk_reserve(
            &self.disk,
            &self.root,
            DiskKind::Content,
            focal_memory::BudgetLane::Completion,
            key.file_bytes(transfer.encoded.len())?
                .checked_add(key.record_bytes())
                .ok_or(ContentError::Capacity)?,
        )?;
        durable_directory(&directory)?;
        let key = self.keys.writable(&directory, domain, key)?;
        key.install(
            &key.path(&directory, transfer.reference.root, sealing::Kind::Manifest)?,
            &transfer.encoded,
        )?;
        manifest.commit();
        Ok(Some(transfer.reference.clone()))
    }
    /// Whether this store holds chunk `index` of `transfer` verified: read
    /// into `scratch` and checked against its name. A chunk absent, or held
    /// with other bytes, is not held — an import installs verified bytes
    /// over it (24 §20). A slice of a transfer's inventory (the audit's F51).
    pub fn holds_chunk(
        &self,
        transfer: &TransferManifest,
        index: usize,
        scratch: &mut [u8],
    ) -> Result<bool, ContentError> {
        self.check()?;
        let chunk = transfer
            .manifest
            .chunks
            .get(index)
            .ok_or(ContentError::Invalid)?;
        let domain = transfer.reference.domain;
        let directory = domain_directory(&self.root, domain);
        let key = self.keys.domain(&directory, domain, false)?;
        if key.holds_nothing() {
            return Ok(false);
        }
        match verify_chunk_into(&directory, &key, chunk.hash, chunk.length as usize, scratch) {
            Ok(_) => Ok(true),
            Err(ContentError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(false)
            }
            Err(ContentError::Corrupt) => Ok(false),
            Err(error) => Err(error),
        }
    }
}

/// A transfer's completion in slices (the audit's F51): what
/// [`ContentStore::complete_import`] does at once — every chunk read back
/// and verified, the stream's digest over them in order, then the manifest
/// installed — one chunk a call ([`ContentStore::advance_completion`]), so
/// the store's one owner serves other work between chunks. Begun for one
/// transfer's root; nothing is installed before the last chunk and the stream
/// verify.
pub struct ImportCompletion {
    root: ContentHash,
    next: usize,
    received: u64,
    whole: blake3::Hasher,
}
impl ImportCompletion {
    /// The chunks verified so far.
    pub fn verified(&self) -> usize {
        self.next
    }
}

impl ContentReader {
    /// Describe an installed object from its manifest alone: the reference
    /// is derived from the manifest the root names, so a caller that knows
    /// only the root (a bundle header, a backup inventory) can export the
    /// exact authenticated tree.
    pub fn describe_object(
        &self,
        domain: ContentDomainId,
        root: ContentHash,
    ) -> Result<TransferManifest, ContentError> {
        let encoded = manifest_plain(
            &self.root,
            &self.keys(),
            domain,
            root,
            MAX_TRANSFER_MANIFEST_BYTES,
        )?;
        describe_encoded(domain, root, encoded)
    }
    /// One authenticated chunk of an object described by `describe_object`.
    pub fn read_transfer_chunk(
        &self,
        transfer: &TransferManifest,
        index: usize,
    ) -> Result<Vec<u8>, ContentError> {
        let chunk = transfer
            .manifest
            .chunks
            .get(index)
            .ok_or(ContentError::Invalid)?;
        if chunk.length as usize > MAX_TRANSFER_CHUNK_BYTES {
            return Err(ContentError::Capacity);
        }
        let domain = transfer.reference.domain;
        let directory = domain_directory(&self.root, domain);
        let key = self.keys().domain(&directory, domain, false)?;
        let mut bytes = zeroed_buffer(chunk.length as usize)?;
        verify_chunk_into(
            &directory,
            &key,
            chunk.hash,
            chunk.length as usize,
            &mut bytes,
        )?;
        Ok(bytes)
    }
}

/// Validate an encoded manifest against the root that names it and derive
/// the object's reference from its own fields.
pub fn describe_encoded(
    domain: ContentDomainId,
    root: ContentHash,
    encoded: Vec<u8>,
) -> Result<TransferManifest, ContentError> {
    if encoded.len() > MAX_TRANSFER_MANIFEST_BYTES {
        return Err(ContentError::Capacity);
    }
    if !encoded.starts_with(MANIFEST_MAGIC)
        || ContentHash(*blake3::hash(&encoded).as_bytes()) != root
    {
        return Err(ContentError::Corrupt);
    }
    let manifest = decode_manifest(
        encoded
            .get(MANIFEST_MAGIC.len()..)
            .ok_or(ContentError::Corrupt)?,
    )?;
    let total = manifest
        .chunks
        .iter()
        .try_fold(0u64, |sum, chunk| sum.checked_add(u64::from(chunk.length)))
        .ok_or(ContentError::Corrupt)?;
    if manifest.schema != 1
        || manifest.domain != domain
        || total != manifest.length
        || manifest.length > MAX_TRANSFER_CONTENT_BYTES
        || manifest
            .chunks
            .iter()
            .any(|chunk| chunk.length == 0 || chunk.length as usize > MAX_TRANSFER_CHUNK_BYTES)
    {
        return Err(ContentError::Corrupt);
    }
    let reference = ContentRef {
        domain,
        root,
        length: manifest.length,
        class: manifest.class,
    };
    Ok(TransferManifest {
        reference,
        encoded,
        manifest,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn truncated_manifest_count_is_rejected_before_chunk_allocation() {
        let root = tempfile::tempdir().unwrap();
        let store = ContentStore::open(
            root.path(),
            limits(4),
            &crate::store::test_keys(root.path()),
        )
        .unwrap();
        let domain = ContentDomainId([2; 16]);
        let mut encoded = MANIFEST_MAGIC.to_vec();
        encoded.extend(
            postcard::to_stdvec(&(
                1u16,
                domain,
                ContentClass::Evidence,
                0u64,
                ContentHash([3; 32]),
                usize::MAX,
            ))
            .unwrap(),
        );
        assert!(encoded.len() < 128);
        let reference = ContentRef {
            domain,
            root: ContentHash(*blake3::hash(&encoded).as_bytes()),
            length: 0,
            class: ContentClass::Evidence,
        };
        // Corrupt comes from the pre-allocation count check, rather than a later
        // postcard unexpected-end error after trusting the enormous size_hint.
        assert!(matches!(
            store.prepare_import(reference, encoded),
            Err(ContentError::Corrupt)
        ));
    }

    #[test]
    fn imported_format_bounds_survive_smaller_upload_preferences_and_restart() {
        let source_dir = tempfile::tempdir().unwrap();
        let target_dir = tempfile::tempdir().unwrap();
        let mut source_store = ContentStore::open(
            source_dir.path(),
            limits(8),
            &crate::store::test_keys(source_dir.path()),
        )
        .unwrap();
        let reference = source(&mut source_store);
        let exported = source_store.export_manifest(&reference).unwrap();
        let mut target_limits = limits(1);
        target_limits.max_manifest_bytes = MANIFEST_MAGIC.len();
        target_limits.max_content_bytes = 4;
        assert!(reference.length > target_limits.max_content_bytes);
        assert!(exported.encoded().len() > target_limits.max_manifest_bytes);
        assert!(exported.chunk_length(0).unwrap() > target_limits.chunk_bytes);
        {
            let mut target = ContentStore::open(
                target_dir.path(),
                target_limits.clone(),
                &crate::store::test_keys(target_dir.path()),
            )
            .unwrap();
            let imported = target
                .prepare_import(reference.clone(), exported.encoded().to_vec())
                .unwrap();
            for index in 0..exported.chunks() {
                target
                    .import_chunk(
                        &imported,
                        index,
                        &source_store.read_transfer_chunk(&exported, index).unwrap(),
                    )
                    .unwrap();
            }
            assert_eq!(target.complete_import(&imported).unwrap(), reference);
        }
        let reopened = ContentStore::open(
            target_dir.path(),
            target_limits,
            &crate::store::test_keys(target_dir.path()),
        )
        .unwrap();
        assert!(matches!(
            reopened.read_bytes(&reference, 4),
            Err(ContentError::Capacity)
        ));
        assert_eq!(reopened.read_bytes(&reference, 10).unwrap(), b"abcdefghij");
        assert_eq!(reopened.read_range(&reference, 1, 8).unwrap(), b"bcdefghi");
        reopened.verify(&reference).unwrap();
        assert_eq!(
            reopened.export_manifest(&reference).unwrap().encoded(),
            exported.encoded()
        );
    }
    /// A chunk imported in parts is the chunk `import_chunk` would have
    /// installed: in order, an exact retry taking nothing, a gap refused, a
    /// part past the chunk refused, the whole verified before it is
    /// installed and discarded whole where it does not verify; and the
    /// parts of a transfer discarded leave its installed chunks.
    #[test]
    fn a_chunk_imported_in_parts_is_the_chunk_once_whole() {
        let source_dir = tempfile::tempdir().unwrap();
        let target_dir = tempfile::tempdir().unwrap();
        let mut source_store = ContentStore::open(
            source_dir.path(),
            limits(8),
            &crate::store::test_keys(source_dir.path()),
        )
        .unwrap();
        let reference = source(&mut source_store);
        let exported = source_store.export_manifest(&reference).unwrap();
        assert_eq!(exported.chunks(), 2);
        let first = source_store.read_transfer_chunk(&exported, 0).unwrap();
        let second = source_store.read_transfer_chunk(&exported, 1).unwrap();
        let mut target = ContentStore::open(
            target_dir.path(),
            limits(8),
            &crate::store::test_keys(target_dir.path()),
        )
        .unwrap();
        let imported = target
            .prepare_import(reference.clone(), exported.encoded().to_vec())
            .unwrap();
        assert_eq!(target.staged_chunk_bytes(&imported, 0).unwrap(), 0);
        // Two bytes, then three from the start (one new), the same three
        // again, two within, a gap, a part past the end.
        assert_eq!(
            target
                .import_chunk_part(&imported, 0, 0, &first[0..2])
                .unwrap(),
            2
        );
        assert_eq!(
            target
                .import_chunk_part(&imported, 0, 0, &first[0..3])
                .unwrap(),
            3
        );
        assert_eq!(
            target
                .import_chunk_part(&imported, 0, 0, &first[0..3])
                .unwrap(),
            3
        );
        assert_eq!(
            target
                .import_chunk_part(&imported, 0, 1, &first[1..3])
                .unwrap(),
            3
        );
        assert!(matches!(
            target.import_chunk_part(&imported, 0, 4, &first[4..5]),
            Err(ContentError::Invalid)
        ));
        assert!(matches!(
            target.import_chunk_part(&imported, 0, 3, &[0; 6]),
            Err(ContentError::Invalid)
        ));
        assert!(matches!(
            target.import_chunk_part(&imported, 0, 3, &[]),
            Err(ContentError::Invalid)
        ));
        assert_eq!(target.staged_chunk_bytes(&imported, 0).unwrap(), 3);
        assert!(target.read_transfer_chunk(&imported, 0).is_err());
        // The rest: the chunk is installed, as import_chunk installs one.
        assert_eq!(
            target
                .import_chunk_part(&imported, 0, 3, &first[3..8])
                .unwrap(),
            8
        );
        assert_eq!(target.staged_chunk_bytes(&imported, 0).unwrap(), 0);
        assert_eq!(target.read_transfer_chunk(&imported, 0).unwrap(), first);
        // A part of an installed chunk adds nothing: held whole.
        assert_eq!(
            target
                .import_chunk_part(&imported, 0, 0, &first[0..1])
                .unwrap(),
            8
        );
        // A last part that is not the chunk's: discarded whole.
        assert_eq!(
            target
                .import_chunk_part(&imported, 1, 0, &second[0..1])
                .unwrap(),
            1
        );
        assert!(matches!(
            target.import_chunk_part(&imported, 1, 1, &[0]),
            Err(ContentError::Corrupt)
        ));
        assert_eq!(target.staged_chunk_bytes(&imported, 1).unwrap(), 0);
        assert!(target.read_transfer_chunk(&imported, 1).is_err());
        // Parts discarded with their transfer; the chunk installed stays.
        assert_eq!(
            target
                .import_chunk_part(&imported, 1, 0, &second[0..1])
                .unwrap(),
            1
        );
        target.discard_chunk_parts(&imported).unwrap();
        assert_eq!(target.staged_chunk_bytes(&imported, 1).unwrap(), 0);
        assert_eq!(target.read_transfer_chunk(&imported, 0).unwrap(), first);
        // Whole by parts, the import completes as any.
        assert_eq!(
            target.import_chunk_part(&imported, 1, 0, &second).unwrap(),
            2
        );
        assert_eq!(target.complete_import(&imported).unwrap(), reference);
        assert_eq!(target.read_bytes(&reference, 10).unwrap(), b"abcdefghij");
    }
    fn limits(chunk_bytes: usize) -> StoreLimits {
        StoreLimits {
            max_content_bytes: 1024,
            max_staging_bytes: 1024,
            max_uploads: 4,
            chunk_bytes,
            max_manifest_bytes: 1024,
        }
    }
    fn source(store: &mut ContentStore) -> ContentRef {
        let upload = UploadId([1; 16]);
        store
            .begin(
                upload,
                ContentDomainId([2; 16]),
                ContentClass::Evidence,
                10,
                None,
            )
            .unwrap();
        store.append(upload, 0, b"abcd").unwrap();
        store.append(upload, 4, b"efgh").unwrap();
        store.append(upload, 8, b"ij").unwrap();
        store.seal(upload).unwrap()
    }
    /// A manifest is charged by its length before it is read
    /// (`manifest_length`): decoded for an export or a verification it holds
    /// no more than `resident_for` that length, at one chunk or at many, and
    /// read within less than its length it is refused before it is read. A
    /// seal holds no more than its chunks charge, and the chunks are read by
    /// the names the manifest gives them.
    #[test]
    fn a_manifest_holds_no_more_than_its_length_charges() {
        for (chunk_bytes, object) in [
            (4, &b"ab"[..]),
            (4, &b"abcdefghij"[..]),
            (1, &b"abcdefghijklmnopqrst"[..]),
        ] {
            let root = tempfile::tempdir().unwrap();
            let mut store = ContentStore::open(
                root.path(),
                limits(chunk_bytes),
                &crate::store::test_keys(root.path()),
            )
            .unwrap();
            let upload = UploadId([1; 16]);
            let domain = ContentDomainId([2; 16]);
            store
                .begin(
                    upload,
                    domain,
                    ContentClass::Evidence,
                    object.len() as u64,
                    None,
                )
                .unwrap();
            for (index, part) in object.chunks(chunk_bytes).enumerate() {
                store
                    .append(upload, (index * chunk_bytes) as u64, part)
                    .unwrap();
            }
            let charge = store.seal_charge(upload).unwrap();
            let sealing = store.begin_seal(upload).unwrap();
            assert!(sealing.resident_bytes().unwrap() <= charge);
            drop(sealing);
            let reference = store.seal(upload).unwrap();
            let encoded = store.manifest_length(&reference).unwrap();
            // The length a holder charges is the manifest's plaintext, read from its sealed
            // file's stated stream: what the export opens.
            assert_eq!(
                encoded,
                store.export_manifest(&reference).unwrap().encoded().len()
            );
            let manifest = store.export_manifest_within(&reference, encoded).unwrap();
            assert_eq!(manifest.chunks(), object.len().div_ceil(chunk_bytes));
            assert!(
                manifest.resident_bytes().unwrap()
                    <= TransferManifest::resident_for(encoded).unwrap()
            );
            let verification = store.begin_verify_within(&reference, encoded).unwrap();
            assert!(
                verification.resident_bytes().unwrap()
                    <= ObjectVerification::resident_for(encoded).unwrap()
            );
            assert!(matches!(
                store.export_manifest_within(&reference, encoded - 1),
                Err(ContentError::Capacity)
            ));
            assert!(matches!(
                store.begin_verify_within(&reference, encoded - 1),
                Err(ContentError::Capacity)
            ));
            let mut read = Vec::new();
            for index in 0..manifest.chunks() {
                let (hash, length) = manifest.chunk(index).unwrap();
                read.extend(store.read_chunk(domain, hash, length as usize).unwrap());
            }
            assert_eq!(read, object);
            // A name read at another length, or under another domain, is
            // refused.
            let (hash, length) = manifest.chunk(0).unwrap();
            assert!(store.read_chunk(domain, hash, length as usize + 1).is_err());
            assert!(
                store
                    .read_chunk(ContentDomainId([3; 16]), hash, length as usize)
                    .is_err()
            );
        }
    }
    #[test]
    fn interrupted_transfer_reuses_durable_chunks_and_preserves_the_original_root() {
        let source_dir = tempfile::tempdir().unwrap();
        let target_dir = tempfile::tempdir().unwrap();
        let mut sender = ContentStore::open(
            source_dir.path(),
            limits(4),
            &crate::store::test_keys(source_dir.path()),
        )
        .unwrap();
        let reference = source(&mut sender);
        let descriptor = sender.export_manifest(&reference).unwrap();
        {
            let mut receiver = ContentStore::open(
                target_dir.path(),
                limits(8),
                &crate::store::test_keys(target_dir.path()),
            )
            .unwrap();
            let imported = receiver
                .prepare_import(reference.clone(), descriptor.encoded().to_vec())
                .unwrap();
            receiver
                .import_chunk(
                    &imported,
                    0,
                    &sender.read_transfer_chunk(&descriptor, 0).unwrap(),
                )
                .unwrap();
            assert!(matches!(
                receiver.complete_import(&imported),
                Err(ContentError::Incomplete {
                    received: 4,
                    expected: 10
                })
            ));
            // An ordinary incomplete transfer does not fail the writer.
            receiver
                .import_chunk(
                    &imported,
                    0,
                    &sender.read_transfer_chunk(&descriptor, 0).unwrap(),
                )
                .unwrap();
            assert!(receiver.verify(&reference).is_err());
        }
        let mut receiver = ContentStore::open(
            target_dir.path(),
            limits(8),
            &crate::store::test_keys(target_dir.path()),
        )
        .unwrap();
        let imported = receiver
            .prepare_import(reference.clone(), descriptor.encoded().to_vec())
            .unwrap();
        assert_eq!(receiver.read_transfer_chunk(&imported, 0).unwrap(), b"abcd");
        for index in 0..descriptor.chunks() {
            let bytes = sender.read_transfer_chunk(&descriptor, index).unwrap();
            receiver.import_chunk(&imported, index, &bytes).unwrap();
        }
        assert_eq!(receiver.complete_import(&imported).unwrap(), reference);
        assert_eq!(receiver.complete_import(&imported).unwrap(), reference);
        drop(receiver);
        let receiver = ContentStore::open(
            target_dir.path(),
            limits(8),
            &crate::store::test_keys(target_dir.path()),
        )
        .unwrap();
        assert_eq!(receiver.read_bytes(&reference, 10).unwrap(), b"abcdefghij");
    }
    #[test]
    fn invalid_manifest_chunk_and_missing_prefix_never_install_a_reference() {
        let source_dir = tempfile::tempdir().unwrap();
        let target_dir = tempfile::tempdir().unwrap();
        let mut sender = ContentStore::open(
            source_dir.path(),
            limits(4),
            &crate::store::test_keys(source_dir.path()),
        )
        .unwrap();
        let reference = source(&mut sender);
        let descriptor = sender.export_manifest(&reference).unwrap();
        let mut receiver = ContentStore::open(
            target_dir.path(),
            limits(8),
            &crate::store::test_keys(target_dir.path()),
        )
        .unwrap();
        let mut corrupted = descriptor.encoded().to_vec();
        corrupted[0] ^= 1;
        assert!(
            receiver
                .prepare_import(reference.clone(), corrupted)
                .is_err()
        );
        let imported = receiver
            .prepare_import(reference.clone(), descriptor.encoded().to_vec())
            .unwrap();
        assert!(matches!(
            receiver.import_chunk(&imported, 0, b"evil"),
            Err(ContentError::Corrupt)
        ));
        assert!(receiver.complete_import(&imported).is_err());
        assert!(receiver.verify(&reference).is_err());
        assert!(receiver.read_transfer_chunk(&imported, usize::MAX).is_err());
    }
    /// The audit's F59: an exact retry of a chunk, a resumed transfer's chunk
    /// already held, and a repeated completion add no byte to the volume, so
    /// none lowers the free-space estimate; only the first installation of
    /// each payload does.
    #[test]
    fn duplicate_chunk_imports_and_a_repeated_completion_charge_the_volume_once() {
        let source_dir = tempfile::tempdir().unwrap();
        let target_dir = tempfile::tempdir().unwrap();
        let mut sender = ContentStore::open(
            source_dir.path(),
            limits(4),
            &crate::store::test_keys(source_dir.path()),
        )
        .unwrap();
        let reference = source(&mut sender);
        let descriptor = sender.export_manifest(&reference).unwrap();
        let disk = DiskBudget::new(DiskBudgetConfig {
            headroom: 0,
            completion_reserve: 0,
            sample_interval: 64,
        })
        .unwrap();
        // Exactly the sealed payload, the sealed manifest, the domain's key
        // record and four more bytes (29 §11): a duplicate that charged the
        // estimate would leave the completion without room.
        let sealed = |plain: usize| focal_seal::file::sealed_bound(plain).unwrap() as u64;
        let record = hyper_seal::keys::RECORD as u64;
        let chunks_sealed: u64 = (0..descriptor.chunks())
            .map(|index| sealed(descriptor.chunk_length(index).unwrap()))
            .sum();
        let manifest_bytes = sealed(descriptor.encoded().len());
        disk.observe(record + chunks_sealed + manifest_bytes + 4);
        let mut receiver = ContentStore::open_with_disk(
            target_dir.path(),
            limits(8),
            disk.clone(),
            &crate::store::test_keys(target_dir.path()),
            ContentForm::Sealed,
        )
        .unwrap();
        let imported = receiver
            .prepare_import(reference.clone(), descriptor.encoded().to_vec())
            .unwrap();
        let chunk = sender.read_transfer_chunk(&descriptor, 0).unwrap();
        let before = disk.uncommitted_free();
        receiver.import_chunk(&imported, 0, &chunk).unwrap();
        let once = disk.uncommitted_free();
        assert_eq!(once, before - sealed(chunk.len()) - record);
        for _ in 0..4 {
            receiver.import_chunk(&imported, 0, &chunk).unwrap();
        }
        assert_eq!(
            disk.uncommitted_free(),
            once,
            "four exact retries added no byte"
        );
        for index in 1..descriptor.chunks() {
            let bytes = sender.read_transfer_chunk(&descriptor, index).unwrap();
            receiver.import_chunk(&imported, index, &bytes).unwrap();
        }
        assert_eq!(disk.uncommitted_free(), before - record - chunks_sealed);
        assert_eq!(receiver.complete_import(&imported).unwrap(), reference);
        let completed = disk.uncommitted_free();
        assert_eq!(completed, before - record - chunks_sealed - manifest_bytes);
        assert_eq!(receiver.complete_import(&imported).unwrap(), reference);
        assert_eq!(
            disk.uncommitted_free(),
            completed,
            "a repeated completion added no byte"
        );
        assert_eq!(receiver.read_bytes(&reference, 10).unwrap(), b"abcdefghij");
        assert_eq!(disk.stats().outstanding, 0);
    }

    /// Chunks of several records each, so a part is records too (29 §12.7).
    const BIG_CHUNK: usize = 3 * record_file::RECORD_BYTES + 777;

    fn big_limits() -> StoreLimits {
        StoreLimits {
            max_content_bytes: 4 * BIG_CHUNK as u64,
            max_staging_bytes: 8 * BIG_CHUNK as u64,
            max_uploads: 4,
            chunk_bytes: BIG_CHUNK,
            max_manifest_bytes: 4096,
        }
    }
    fn big_store(root: &Path, form: ContentForm) -> ContentStore {
        ContentStore::open_with_disk(
            root,
            big_limits(),
            focal_memory::DiskBudget::new(focal_memory::DiskBudgetConfig::unbounded()).unwrap(),
            &crate::store::test_keys(root),
            form,
        )
        .unwrap()
    }
    fn patterned(seed: u8, length: usize) -> Vec<u8> {
        (0..length)
            .map(|index| (index.wrapping_mul(53) ^ (index >> 6)) as u8 ^ seed)
            .collect()
    }
    /// A source store's object of two chunks, its manifest exported, and a receiver's import of it
    /// prepared in `form`.
    fn big_transfer(
        root: &Path,
        form: ContentForm,
    ) -> (
        tempfile::TempDir,
        ContentStore,
        TransferManifest,
        Vec<Vec<u8>>,
    ) {
        let source_dir = tempfile::tempdir().unwrap();
        let mut source = big_store(source_dir.path(), ContentForm::Sealed);
        let bytes = patterned(4, 2 * BIG_CHUNK);
        let upload = UploadId([3; 16]);
        source
            .begin(
                upload,
                ContentDomainId([2; 16]),
                ContentClass::Evidence,
                bytes.len() as u64,
                None,
            )
            .unwrap();
        for (index, piece) in bytes.chunks(BIG_CHUNK).enumerate() {
            source
                .append(upload, (index * BIG_CHUNK) as u64, piece)
                .unwrap();
        }
        let reference = source.seal(upload).unwrap();
        let exported = source.export_manifest(&reference).unwrap();
        let chunks = (0..exported.chunks())
            .map(|index| source.read_transfer_chunk(&exported, index).unwrap())
            .collect();
        let receiver = big_store(root, form);
        let imported = receiver
            .prepare_import(reference, exported.encoded().to_vec())
            .unwrap();
        (source_dir, receiver, imported, chunks)
    }
    /// A part of `chunk` sent from `from` to `to`.
    fn send(
        store: &mut ContentStore,
        transfer: &TransferManifest,
        index: usize,
        chunk: &[u8],
        from: usize,
        to: usize,
    ) -> u64 {
        store
            .import_chunk_part(transfer, index, from as u64, &chunk[from..to])
            .unwrap()
    }
    /// The files of the receiver's domain: none named by a chunk's hash, none holding 16 bytes of a
    /// chunk.
    fn assert_parts_sealed(root: &Path, transfer: &TransferManifest, chunks: &[Vec<u8>]) {
        let directory = domain_directory(root, transfer.reference.domain);
        for entry in std::fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            let stored = crate::store::stored_bytes(&path);
            for chunk in &transfer.manifest.chunks {
                assert!(
                    !name.contains(&chunk.hash.to_string()),
                    "{name} names a chunk"
                );
            }
            for chunk in chunks {
                for probe in chunk.chunks(4096).map(|piece| &piece[..16]) {
                    assert!(
                        !stored.windows(16).any(|w| w == probe),
                        "{name} holds plaintext"
                    );
                }
            }
        }
    }

    /// Do: receive a chunk in parts into a store that seals, stop it halfway, open it again and
    /// send the rest. Expect: the part goes on at the bytes it held, the chunk is installed whole,
    /// and no file of the domain is named by a chunk's hash or holds its bytes in the clear.
    #[test]
    fn a_chunk_received_in_parts_sealed_goes_on_after_a_restart() {
        let root = tempfile::tempdir().unwrap();
        let (_source, mut receiver, imported, chunks) =
            big_transfer(root.path(), ContentForm::Sealed);
        let chunk = &chunks[0];
        assert_eq!(send(&mut receiver, &imported, 0, chunk, 0, 70_000), 70_000);
        assert_eq!(
            send(&mut receiver, &imported, 0, chunk, 70_000, 150_000),
            150_000
        );
        assert_parts_sealed(root.path(), &imported, &chunks);
        drop(receiver);
        let mut receiver = big_store(root.path(), ContentForm::Sealed);
        assert_eq!(receiver.staged_chunk_bytes(&imported, 0).unwrap(), 150_000);
        // A retry within what is held, then a gap, then the rest.
        assert_eq!(
            send(&mut receiver, &imported, 0, chunk, 100_000, 140_000),
            150_000
        );
        assert!(matches!(
            receiver.import_chunk_part(&imported, 0, 160_000, &chunk[160_000..170_000]),
            Err(ContentError::Invalid)
        ));
        assert_eq!(
            send(&mut receiver, &imported, 0, chunk, 150_000, chunk.len()),
            chunk.len() as u64
        );
        assert_eq!(&receiver.read_transfer_chunk(&imported, 0).unwrap(), chunk);
        assert_parts_sealed(root.path(), &imported, &chunks);
    }

    /// Do: put one chunk's sealed part under another chunk's name, and cut a part's last record
    /// short. Expect: the moved part opens to nothing (its key is kept for its chunk) and is
    /// discarded; the cut part goes on at its whole records' bytes under a new key, and the chunk
    /// installs whole.
    #[test]
    fn a_moved_part_is_refused_and_a_cut_part_goes_on_under_a_new_key() {
        let root = tempfile::tempdir().unwrap();
        let (_source, mut receiver, imported, chunks) =
            big_transfer(root.path(), ContentForm::Sealed);
        send(&mut receiver, &imported, 0, &chunks[0], 0, 90_000);
        send(&mut receiver, &imported, 1, &chunks[1], 0, 90_000);
        let directory = domain_directory(root.path(), imported.reference.domain);
        let key = receiver
            .keys
            .domain(&directory, imported.reference.domain, false)
            .unwrap();
        let part = |index: usize| {
            key.path(
                &directory,
                imported.manifest.chunks[index].hash,
                sealing::Kind::Part,
            )
            .unwrap()
        };
        std::fs::copy(part(0), part(1)).unwrap();
        assert!(matches!(
            receiver.import_chunk_part(&imported, 1, 90_000, &chunks[1][90_000..100_000]),
            Err(ContentError::Corrupt)
        ));
        assert!(!part(1).exists(), "the moved part was discarded");
        assert_eq!(receiver.staged_chunk_bytes(&imported, 1).unwrap(), 0);
        // Chunk 0's part, its last record cut short.
        let before = std::fs::read(part(0)).unwrap()[8..24].to_vec();
        let length = std::fs::metadata(part(0)).unwrap().len();
        std::fs::OpenOptions::new()
            .write(true)
            .open(part(0))
            .unwrap()
            .set_len(length - 1000)
            .unwrap();
        let held = receiver.staged_chunk_bytes(&imported, 0).unwrap();
        assert!(
            held < 90_000 && held >= record_file::RECORD_BYTES as u64,
            "{held}"
        );
        assert_ne!(
            std::fs::read(part(0)).unwrap()[8..24].to_vec(),
            before,
            "a new key"
        );
        let chunk = &chunks[0];
        send(
            &mut receiver,
            &imported,
            0,
            chunk,
            held as usize,
            chunk.len(),
        );
        assert_eq!(&receiver.read_transfer_chunk(&imported, 0).unwrap(), chunk);
    }

    /// Do: stage part of a chunk in a store in the clear, open it sealing and convert it, then send
    /// the rest. Expect: the conversion seals the part under its keyed name (counted and converted),
    /// the transfer goes on in it, and the chunk installs whole.
    #[test]
    fn a_part_in_the_clear_is_converted_and_its_transfer_goes_on() {
        let root = tempfile::tempdir().unwrap();
        let (_source, mut receiver, imported, chunks) =
            big_transfer(root.path(), ContentForm::Clear);
        let chunk = &chunks[0];
        send(&mut receiver, &imported, 0, chunk, 0, 80_000);
        drop(receiver);
        let mut receiver = big_store(root.path(), ContentForm::Sealed);
        let mut scratch = vec![0u8; crate::MAX_TRANSFER_CHUNK_BYTES];
        for _ in 0..10_000 {
            if receiver.convert_step(&mut scratch, 1).unwrap() == crate::Conversion::Done {
                break;
            }
        }
        let status = receiver.conversion_status();
        assert_eq!(status.form, crate::StoreForm::Sealed);
        assert_eq!(status.left(), crate::Tally::default());
        assert!(status.converted.bytes >= 80_000);
        assert_parts_sealed(root.path(), &imported, &chunks);
        assert_eq!(receiver.staged_chunk_bytes(&imported, 0).unwrap(), 80_000);
        send(&mut receiver, &imported, 0, chunk, 80_000, chunk.len());
        assert_eq!(&receiver.read_transfer_chunk(&imported, 0).unwrap(), chunk);
    }
}
