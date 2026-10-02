//! Exact content-tree transfer. A receiver preserves the sender's manifest and
//! chunk boundaries, independent of its preferred upload chunk size.
use super::*;

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
}

impl ContentStore {
    /// Exports the exact authenticated tree rather than rechunking its bytes.
    pub fn export_manifest(
        &self,
        reference: &ContentRef,
    ) -> Result<TransferManifest, ContentError> {
        self.check()?;
        let path = self
            .root
            .join("objects")
            .join(hex(&reference.domain.0))
            .join(format!("{}.manifest", reference.root));
        let encoded = read_bounded(&path, MAX_TRANSFER_MANIFEST_BYTES)?;
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
        let path = self
            .root
            .join("objects")
            .join(hex(&transfer.reference.domain.0))
            .join(format!("{}.chunk", chunk.hash));
        let bytes = read_bounded(&path, chunk.length as usize)?;
        if bytes.len() != chunk.length as usize
            || ContentHash(*blake3::hash(&bytes).as_bytes()) != chunk.hash
        {
            return Err(ContentError::Corrupt);
        }
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
            let directory = self
                .root
                .join("objects")
                .join(hex(&transfer.reference.domain.0));
            let path = directory.join(format!("{}.chunk", chunk.hash));
            // A verified duplicate — an exact retry, or a resumed transfer's
            // chunk already held — adds no byte to the volume: it needs no
            // promise and lowers no estimate (the audit's F59).
            if already_installed(&path, bytes, Some(chunk.hash))? {
                return Ok(());
            }
            // Custody transfers are fleet work: they draw on the completion
            // lane so a tenant's uploads cannot starve replication.
            let copy = disk_reserve(
                &self.disk,
                &self.root,
                DiskKind::Content,
                focal_memory::BudgetLane::Completion,
                u64::try_from(bytes.len()).map_err(|_| ContentError::Capacity)?,
            )?;
            durable_directory(&directory)?;
            atomic_install(&path, bytes)?;
            copy.commit();
            Ok(())
        })();
        self.mark_failure(&result);
        result
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
        let path = self.chunk_part_path(transfer, chunk.hash);
        match std::fs::metadata(&path) {
            Ok(metadata) => Ok(metadata.len().min(u64::from(chunk.length))),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(error) => Err(error.into()),
        }
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
            let directory = self
                .root
                .join("objects")
                .join(hex(&transfer.reference.domain.0));
            let installed = directory.join(format!("{}.chunk", chunk.hash));
            if installed.exists() && self.read_transfer_chunk(transfer, index).is_ok() {
                // Held whole already (F59): the part adds nothing.
                return Ok(length);
            }
            let path = self.chunk_part_path(transfer, chunk.hash);
            let staged = match std::fs::metadata(&path) {
                Ok(metadata) => metadata.len(),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
                Err(error) => return Err(error.into()),
            };
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
            let promise = disk_reserve(
                &self.disk,
                &self.root,
                DiskKind::Content,
                focal_memory::BudgetLane::Completion,
                u64::try_from(fresh.len()).map_err(|_| ContentError::Capacity)?,
            )?;
            durable_directory(&directory)?;
            {
                let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
                file.write_all(fresh)?;
                file.sync_all()?;
            }
            promise.commit();
            if end < length {
                return Ok(end);
            }
            // Whole: verified, then the staged bytes become the chunk.
            let held = read_bounded(&path, chunk.length as usize)?;
            if held.len() != chunk.length as usize
                || ContentHash(*blake3::hash(&held).as_bytes()) != chunk.hash
            {
                let _ = std::fs::remove_file(&path);
                sync_directory(&directory)?;
                return Err(ContentError::Corrupt);
            }
            focal_platform::fs::atomic_replace(&path, &installed)?;
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
                let path = self.chunk_part_path(transfer, chunk.hash);
                match std::fs::remove_file(&path) {
                    Ok(()) => removed = true,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
            }
            if removed {
                sync_directory(&directory)?;
            }
            Ok(())
        })();
        self.mark_failure(&result);
        result
    }
    fn chunk_part_path(
        &self,
        transfer: &TransferManifest,
        hash: ContentHash,
    ) -> std::path::PathBuf {
        self.root
            .join("objects")
            .join(hex(&transfer.reference.domain.0))
            .join(format!("{hash}.part"))
    }

    /// Verifies every chunk and the whole stream before publishing the original
    /// manifest. Success proves local durable custody of exactly this root.
    pub fn complete_import(
        &mut self,
        transfer: &TransferManifest,
    ) -> Result<ContentRef, ContentError> {
        self.check()?;
        let result = (|| {
            if transfer.encoded.len() > MAX_TRANSFER_MANIFEST_BYTES
                || transfer.reference.length > MAX_TRANSFER_CONTENT_BYTES
            {
                return Err(ContentError::Capacity);
            }
            let mut whole = blake3::Hasher::new();
            let mut received = 0u64;
            for index in 0..transfer.manifest.chunks.len() {
                let bytes = match self.read_transfer_chunk(transfer, index) {
                    Err(ContentError::Io(error))
                        if error.kind() == std::io::ErrorKind::NotFound =>
                    {
                        return Err(ContentError::Incomplete {
                            received,
                            expected: transfer.reference.length,
                        });
                    }
                    result => result?,
                };
                received = received
                    .checked_add(bytes.len() as u64)
                    .ok_or(ContentError::Corrupt)?;
                whole.update(&bytes);
            }
            if ContentHash(*whole.finalize().as_bytes()) != transfer.manifest.stream_digest {
                return Err(ContentError::Corrupt);
            }
            let directory = self
                .root
                .join("objects")
                .join(hex(&transfer.reference.domain.0));
            let path = directory.join(format!("{}.manifest", transfer.reference.root));
            // A repeated completion finds its manifest installed: no promise,
            // no byte.
            if already_installed(&path, &transfer.encoded, Some(transfer.reference.root))? {
                return Ok(transfer.reference.clone());
            }
            let manifest = disk_reserve(
                &self.disk,
                &self.root,
                DiskKind::Content,
                focal_memory::BudgetLane::Completion,
                u64::try_from(transfer.encoded.len()).map_err(|_| ContentError::Capacity)?,
            )?;
            durable_directory(&directory)?;
            atomic_install(&path, &transfer.encoded)?;
            manifest.commit();
            Ok(transfer.reference.clone())
        })();
        self.mark_failure(&result);
        result
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
        let path = self
            .root
            .join("objects")
            .join(hex(&domain.0))
            .join(format!("{root}.manifest"));
        let encoded = read_bounded(&path, MAX_TRANSFER_MANIFEST_BYTES)?;
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
        let path = self
            .root
            .join("objects")
            .join(hex(&transfer.reference.domain.0))
            .join(format!("{}.chunk", chunk.hash));
        let bytes = read_bounded(&path, chunk.length as usize)?;
        if bytes.len() != chunk.length as usize
            || ContentHash(*blake3::hash(&bytes).as_bytes()) != chunk.hash
        {
            return Err(ContentError::Corrupt);
        }
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
        let store = ContentStore::open(root.path(), limits(4)).unwrap();
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
        let mut source_store = ContentStore::open(source_dir.path(), limits(8)).unwrap();
        let reference = source(&mut source_store);
        let exported = source_store.export_manifest(&reference).unwrap();
        let mut target_limits = limits(1);
        target_limits.max_manifest_bytes = MANIFEST_MAGIC.len();
        target_limits.max_content_bytes = 4;
        assert!(reference.length > target_limits.max_content_bytes);
        assert!(exported.encoded().len() > target_limits.max_manifest_bytes);
        assert!(exported.chunk_length(0).unwrap() > target_limits.chunk_bytes);
        {
            let mut target = ContentStore::open(target_dir.path(), target_limits.clone()).unwrap();
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
        let reopened = ContentStore::open(target_dir.path(), target_limits).unwrap();
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
        let mut source_store = ContentStore::open(source_dir.path(), limits(8)).unwrap();
        let reference = source(&mut source_store);
        let exported = source_store.export_manifest(&reference).unwrap();
        assert_eq!(exported.chunks(), 2);
        let first = source_store.read_transfer_chunk(&exported, 0).unwrap();
        let second = source_store.read_transfer_chunk(&exported, 1).unwrap();
        let mut target = ContentStore::open(target_dir.path(), limits(8)).unwrap();
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
    #[test]
    fn interrupted_transfer_reuses_durable_chunks_and_preserves_the_original_root() {
        let source_dir = tempfile::tempdir().unwrap();
        let target_dir = tempfile::tempdir().unwrap();
        let mut sender = ContentStore::open(source_dir.path(), limits(4)).unwrap();
        let reference = source(&mut sender);
        let descriptor = sender.export_manifest(&reference).unwrap();
        {
            let mut receiver = ContentStore::open(target_dir.path(), limits(8)).unwrap();
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
        let mut receiver = ContentStore::open(target_dir.path(), limits(8)).unwrap();
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
        let receiver = ContentStore::open(target_dir.path(), limits(8)).unwrap();
        assert_eq!(receiver.read_bytes(&reference, 10).unwrap(), b"abcdefghij");
    }
    #[test]
    fn invalid_manifest_chunk_and_missing_prefix_never_install_a_reference() {
        let source_dir = tempfile::tempdir().unwrap();
        let target_dir = tempfile::tempdir().unwrap();
        let mut sender = ContentStore::open(source_dir.path(), limits(4)).unwrap();
        let reference = source(&mut sender);
        let descriptor = sender.export_manifest(&reference).unwrap();
        let mut receiver = ContentStore::open(target_dir.path(), limits(8)).unwrap();
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
        let mut sender = ContentStore::open(source_dir.path(), limits(4)).unwrap();
        let reference = source(&mut sender);
        let descriptor = sender.export_manifest(&reference).unwrap();
        let disk = DiskBudget::new(DiskBudgetConfig {
            headroom: 0,
            completion_reserve: 0,
            sample_interval: 64,
        })
        .unwrap();
        // Exactly the payload, the manifest and four more bytes: a duplicate
        // that charged the estimate would leave the completion without room.
        let manifest_bytes = descriptor.encoded().len() as u64;
        disk.observe(reference.length + manifest_bytes + 4);
        let mut receiver =
            ContentStore::open_with_disk(target_dir.path(), limits(8), disk.clone()).unwrap();
        let imported = receiver
            .prepare_import(reference.clone(), descriptor.encoded().to_vec())
            .unwrap();
        let chunk = sender.read_transfer_chunk(&descriptor, 0).unwrap();
        let before = disk.uncommitted_free();
        receiver.import_chunk(&imported, 0, &chunk).unwrap();
        let once = disk.uncommitted_free();
        assert_eq!(once, before - chunk.len() as u64);
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
        assert_eq!(disk.uncommitted_free(), before - reference.length);
        assert_eq!(receiver.complete_import(&imported).unwrap(), reference);
        let completed = disk.uncommitted_free();
        assert_eq!(completed, before - reference.length - manifest_bytes);
        assert_eq!(receiver.complete_import(&imported).unwrap(), reference);
        assert_eq!(
            disk.uncommitted_free(),
            completed,
            "a repeated completion added no byte"
        );
        assert_eq!(receiver.read_bytes(&reference, 10).unwrap(), b"abcdefghij");
        assert_eq!(disk.stats().outstanding, 0);
    }
}
