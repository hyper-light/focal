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
            durable_directory(&directory)?;
            install_verified_chunk(
                &directory.join(format!("{}.chunk", chunk.hash)),
                bytes,
                chunk.hash,
            )
        })();
        self.mark_failure(&result);
        result
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
            durable_directory(&directory)?;
            atomic_install(
                &directory.join(format!("{}.manifest", transfer.reference.root)),
                &transfer.encoded,
            )?;
            Ok(transfer.reference.clone())
        })();
        self.mark_failure(&result);
        result
    }
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
}
