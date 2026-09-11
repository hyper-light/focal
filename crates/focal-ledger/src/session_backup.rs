/// A coherent backup of one native session at a declared prefix (26 §6):
/// the session envelope its replica installed durably, the chunks of a
/// seeded Core root, and the exact authenticated trees of every content
/// object the prefix's rows and archive bundles name, under one manifest
/// written last. A directory without a complete manifest is not a backup.
pub mod backup {
    use super::*;
    use crate::native_checkpoint::{Checkpoint, RetentionSection, SeedChunk};
    use focal_core::native::record_codec::{StructuralArchive, recovery};
    use focal_core::native::{ContentRoot, NativeError};
    use focal_evidence::{
        BuiltinNativeSchemas, ContentError, ContentReader, NativeCustodyReader, SeedReader,
        TransferManifest, describe_encoded,
    };
    use focal_memory::RangeId;
    use std::collections::BTreeSet;
    use std::path::{Path, PathBuf};

    /// `FCLBKUP1`: the manifest's magic; a schema, a postcard body and a
    /// BLAKE3 trailer over both follow.
    pub const MAGIC: &[u8; 8] = b"FCLBKUP1";
    pub const SCHEMA: u16 = 1;
    pub const MANIFEST_FILE: &str = "MANIFEST";
    pub const CHECKPOINT_FILE: &str = "checkpoint";
    pub const SEEDS_DIR: &str = "seeds";
    pub const CONTENT_DIR: &str = "content";
    /// The largest manifest a backup may carry.
    pub const MAX_MANIFEST_BYTES: usize = 64 * 1024 * 1024;
    /// The largest session envelope a backup may carry (the checkpoint's own
    /// bound, 25 §5).
    pub const MAX_CHECKPOINT_BYTES: usize = 8 * 1024 * 1024;
    /// The largest archive bundle read while walking a prefix's proof.
    const MAX_BUNDLE_BYTES: usize = 64 * 1024 * 1024;
    /// Rows visited per page of the roots walk.
    const PAGE: usize = 4096;
    /// Problems a verification reports before it stops listing them.
    const MAX_PROBLEMS: usize = 64;

    #[derive(Debug, thiserror::Error)]
    pub enum BackupError {
        #[error("backup io: {0}")]
        Io(#[from] std::io::Error),
        #[error("ledger: {0}")]
        Ledger(#[from] LedgerError),
        #[error("content: {0}")]
        Content(#[from] ContentError),
        #[error("memory: {0}")]
        Memory(#[from] MemoryError),
        #[error("the output directory already holds a backup")]
        Exists,
        #[error("the backup lacks {0}")]
        Missing(&'static str),
        #[error("the backup is corrupt: {0}")]
        Corrupt(&'static str),
        #[error("the image is not a native session envelope")]
        Unsupported,
        #[error("bounded backup capacity exceeded")]
        Capacity,
    }
    impl From<NativeSessionError> for BackupError {
        fn from(error: NativeSessionError) -> Self {
            Self::Ledger(error.into())
        }
    }
    impl From<crate::native_checkpoint::Error> for BackupError {
        fn from(error: crate::native_checkpoint::Error) -> Self {
            Self::Ledger(NativeSessionError::from(error).into())
        }
    }
    impl From<NativeError> for BackupError {
        fn from(error: NativeError) -> Self {
            Self::Ledger(NativeSessionError::from(error).into())
        }
    }
    impl From<focal_core::native::record_codec::CodecError> for BackupError {
        fn from(_: focal_core::native::record_codec::CodecError) -> Self {
            Self::Corrupt("archive bundle")
        }
    }

    /// One chunk of a seeded Core root or of a content object's tree.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub struct BackupChunk {
        pub hash: ContentHash,
        pub length: u32,
    }
    /// One content object the backup carries as its exact tree.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct BackupObject {
        pub root: ContentHash,
        pub length: u64,
        pub class: ContentClass,
        pub chunks: Vec<BackupChunk>,
    }
    /// What the backup names. Every field is checked again by `verify`.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct BackupManifest {
        pub schema: u16,
        pub created_ms: u64,
        /// The exact committed prefix the envelope was installed at.
        pub prefix: EvidencePrefix,
        /// The native prefix the envelope holds (the legacy prefix is the
        /// evidence prefix's `sequence`).
        pub native_sequence: u64,
        /// The activation's genesis hash (22 §FCNGENES).
        pub native_genesis: ContentHash,
        /// The native content profile, as the activation record encodes it.
        pub profile: u8,
        /// The content domain the objects live in.
        pub domain: ContentDomainId,
        /// The decoder floor the log promised and the successor it carries.
        pub decoder_predecessor: [u8; 32],
        pub decoder_successor: [u8; 32],
        /// The membership the envelope's checkpoint was written under.
        pub configuration: MembershipConfiguration,
        pub checkpoint_hash: ContentHash,
        pub checkpoint_bytes: u64,
        /// The chunks of a seeded Core root, in table order; empty when the
        /// root is inline.
        pub seeds: Vec<BackupChunk>,
        /// Every object the prefix's rows and bundle headers name, sorted by
        /// root.
        pub content: Vec<BackupObject>,
        pub archived_through: u64,
        pub retired_families: u64,
    }
    impl BackupManifest {
        pub fn encode(&self) -> Result<Vec<u8>, BackupError> {
            let body = postcard::to_stdvec(self).map_err(|_| BackupError::Capacity)?;
            let length = body
                .len()
                .checked_add(MAGIC.len())
                .and_then(|n| n.checked_add(32))
                .ok_or(BackupError::Capacity)?;
            if length > MAX_MANIFEST_BYTES {
                return Err(BackupError::Capacity);
            }
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(length)
                .map_err(|_| BackupError::Capacity)?;
            bytes.extend_from_slice(MAGIC);
            bytes.extend_from_slice(&body);
            let digest = blake3::hash(&bytes);
            bytes.extend_from_slice(digest.as_bytes());
            Ok(bytes)
        }
        pub fn decode(bytes: &[u8]) -> Result<Self, BackupError> {
            if bytes.len() > MAX_MANIFEST_BYTES {
                return Err(BackupError::Capacity);
            }
            let body_end = bytes
                .len()
                .checked_sub(32)
                .ok_or(BackupError::Corrupt("manifest trailer"))?;
            let (covered, trailer) = bytes
                .split_at_checked(body_end)
                .ok_or(BackupError::Corrupt("manifest trailer"))?;
            if blake3::hash(covered).as_bytes() != trailer {
                return Err(BackupError::Corrupt("manifest digest"));
            }
            let body = covered
                .strip_prefix(MAGIC.as_slice())
                .ok_or(BackupError::Corrupt("manifest magic"))?;
            let (manifest, rest): (Self, &[u8]) =
                postcard::take_from_bytes(body).map_err(|_| BackupError::Corrupt("manifest body"))?;
            if !rest.is_empty() || manifest.schema != SCHEMA {
                return Err(BackupError::Corrupt("manifest schema"));
            }
            manifest.validate()?;
            Ok(manifest)
        }
        fn validate(&self) -> Result<(), BackupError> {
            let prefix = &self.prefix;
            if prefix.ledger.tenant.is_zero()
                || prefix.ledger.session.is_zero()
                || prefix.index.0 == 0
                || prefix.term.0 == 0
                || prefix.node == 0
                || self.checkpoint_bytes == 0
                || self.checkpoint_bytes > MAX_CHECKPOINT_BYTES as u64
                || prefix.checkpoint != self.checkpoint_hash
                || prefix.checkpoint_bytes != self.checkpoint_bytes
                || self.decoder_predecessor == self.decoder_successor
                || self.configuration.voters.is_empty()
                || self.content.windows(2).any(|pair| {
                    pair.first().zip(pair.get(1)).is_some_and(|(a, b)| a.root >= b.root)
                })
                || self.content.iter().any(|object| {
                    object.root.0 == [0; 32]
                        || object.chunks.is_empty()
                        || object.chunks.iter().any(|chunk| chunk.length == 0)
                        || object
                            .chunks
                            .iter()
                            .try_fold(0u64, |sum, chunk| sum.checked_add(u64::from(chunk.length)))
                            != Some(object.length)
                })
                || self.seeds.iter().any(|chunk| chunk.length == 0)
            {
                return Err(BackupError::Corrupt("manifest fields"));
            }
            Ok(())
        }
        /// Files and bytes the backup holds besides its manifest.
        pub fn totals(&self) -> (u64, u64) {
            let mut files = 1u64;
            let mut bytes = self.checkpoint_bytes;
            for chunk in &self.seeds {
                files = files.saturating_add(1);
                bytes = bytes.saturating_add(u64::from(chunk.length));
            }
            let mut chunks = BTreeSet::new();
            for object in &self.content {
                files = files.saturating_add(1);
                for chunk in &object.chunks {
                    if chunks.insert(chunk.hash) {
                        files = files.saturating_add(1);
                        bytes = bytes.saturating_add(u64::from(chunk.length));
                    }
                }
            }
            (files, bytes)
        }
    }

    /// Where a backup's files go and how they become durable. The real
    /// filesystem and the simulated disk of the qualification suite both
    /// implement it; the install sequence (temporary, sync, rename, sync the
    /// directory) is written once over it.
    pub trait BackupMedium {
        fn create_dir(&mut self, path: &Path) -> std::io::Result<()>;
        fn create(&mut self, path: &Path) -> std::io::Result<()>;
        fn write(&mut self, path: &Path, bytes: &[u8]) -> std::io::Result<()>;
        fn sync_file(&mut self, path: &Path) -> std::io::Result<()>;
        fn sync_dir(&mut self, path: &Path) -> std::io::Result<()>;
        fn rename(&mut self, from: &Path, to: &Path) -> std::io::Result<()>;
        fn exists(&self, path: &Path) -> bool;
        fn read(&self, path: &Path, limit: usize) -> std::io::Result<Vec<u8>>;
    }
    /// The real filesystem.
    #[derive(Debug, Default)]
    pub struct FileMedium;
    impl BackupMedium for FileMedium {
        fn create_dir(&mut self, path: &Path) -> std::io::Result<()> {
            if path.is_dir() {
                return Ok(());
            }
            if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
                self.create_dir(parent)?;
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                std::fs::DirBuilder::new().mode(0o700).create(path)?;
            }
            #[cfg(not(unix))]
            std::fs::create_dir(path)?;
            std::fs::File::open(path)?.sync_all()?;
            if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
                std::fs::File::open(parent)?.sync_all()?;
            }
            Ok(())
        }
        fn create(&mut self, path: &Path) -> std::io::Result<()> {
            std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(path)
                .map(drop)
        }
        fn write(&mut self, path: &Path, bytes: &[u8]) -> std::io::Result<()> {
            use std::io::Write as _;
            let mut file = std::fs::OpenOptions::new().append(true).open(path)?;
            file.write_all(bytes)
        }
        fn sync_file(&mut self, path: &Path) -> std::io::Result<()> {
            std::fs::File::open(path)?.sync_all()
        }
        fn sync_dir(&mut self, path: &Path) -> std::io::Result<()> {
            std::fs::File::open(path)?.sync_all()
        }
        fn rename(&mut self, from: &Path, to: &Path) -> std::io::Result<()> {
            std::fs::rename(from, to)
        }
        fn exists(&self, path: &Path) -> bool {
            path.exists()
        }
        fn read(&self, path: &Path, limit: usize) -> std::io::Result<Vec<u8>> {
            use std::io::Read as _;
            let mut file = std::fs::File::open(path)?;
            let length = usize::try_from(file.metadata()?.len())
                .map_err(|_| std::io::Error::other("file too large"))?;
            if length > limit {
                return Err(std::io::Error::other("file exceeds the bound"));
            }
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(length)
                .map_err(|_| std::io::Error::other("allocation"))?;
            bytes.resize(length, 0);
            file.read_exact(&mut bytes)?;
            Ok(bytes)
        }
    }
    /// Install one file durably: a temporary name, the bytes, a file sync,
    /// the rename, and the directory sync that makes the name durable.
    fn install<M: BackupMedium>(medium: &mut M, path: &Path, bytes: &[u8]) -> Result<(), BackupError> {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .ok_or(BackupError::Corrupt("backup path"))?;
        let temporary = path.with_extension("part");
        medium.create(&temporary)?;
        medium.write(&temporary, bytes)?;
        medium.sync_file(&temporary)?;
        medium.rename(&temporary, path)?;
        medium.sync_dir(parent)?;
        Ok(())
    }

    /// Where the chunks of a seeded Core root are read from: a replica's seed
    /// store while writing, the backup's seed files while verifying.
    pub trait SeedSource {
        fn read_seed(&self, hash: ContentHash, length: usize) -> Result<Vec<u8>, ContentError>;
    }
    impl SeedSource for SeedReader {
        fn read_seed(&self, hash: ContentHash, length: usize) -> Result<Vec<u8>, ContentError> {
            self.read(hash, length)
        }
    }
    /// The seed files of a backup directory, read through a medium.
    pub struct BackupSeeds<'m, M> {
        medium: &'m M,
        root: PathBuf,
    }
    impl<'m, M: BackupMedium> BackupSeeds<'m, M> {
        pub fn new(medium: &'m M, root: &Path) -> Self {
            Self {
                medium,
                root: root.join(SEEDS_DIR),
            }
        }
    }
    impl<M: BackupMedium> SeedSource for BackupSeeds<'_, M> {
        fn read_seed(&self, hash: ContentHash, length: usize) -> Result<Vec<u8>, ContentError> {
            let bytes = self
                .medium
                .read(&self.root.join(format!("{hash}.seed")), length)
                .map_err(ContentError::Io)?;
            if bytes.len() != length || ContentHash(*blake3::hash(&bytes).as_bytes()) != hash {
                return Err(ContentError::Corrupt);
            }
            Ok(bytes)
        }
    }
    /// Where content objects are read from: a node's content store while
    /// writing, the backup's own files while verifying or restoring.
    pub trait ObjectSource: NativeCustodyReader {
        fn describe(
            &self,
            domain: ContentDomainId,
            root: ContentHash,
        ) -> Result<TransferManifest, ContentError>;
        fn chunk(&self, transfer: &TransferManifest, index: usize) -> Result<Vec<u8>, ContentError>;
    }
    impl ObjectSource for ContentReader {
        fn describe(
            &self,
            domain: ContentDomainId,
            root: ContentHash,
        ) -> Result<TransferManifest, ContentError> {
            self.describe_object(domain, root)
        }
        fn chunk(&self, transfer: &TransferManifest, index: usize) -> Result<Vec<u8>, ContentError> {
            self.read_transfer_chunk(transfer, index)
        }
    }
    /// The content files of a backup directory, read through a medium.
    pub struct BackupContent<'m, M> {
        medium: &'m M,
        root: PathBuf,
    }
    impl<'m, M: BackupMedium> BackupContent<'m, M> {
        pub fn new(medium: &'m M, root: &Path) -> Self {
            Self {
                medium,
                root: root.join(CONTENT_DIR),
            }
        }
    }
    impl<M: BackupMedium> NativeCustodyReader for BackupContent<'_, M> {
        fn read_content(&self, reference: &ContentRef, budget: usize) -> Result<Vec<u8>, ContentError> {
            let transfer = self.describe(reference.domain, reference.root)?;
            if transfer.reference() != reference
                || reference.length > budget as u64
            {
                return Err(ContentError::Corrupt);
            }
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(usize::try_from(reference.length).map_err(|_| ContentError::Capacity)?)
                .map_err(|_| ContentError::Capacity)?;
            for index in 0..transfer.chunks() {
                bytes.extend_from_slice(&self.chunk(&transfer, index)?);
            }
            if bytes.len() as u64 != reference.length
                || ContentHash(*blake3::hash(&bytes).as_bytes()) != transfer.stream_digest()
            {
                return Err(ContentError::Corrupt);
            }
            Ok(bytes)
        }
    }
    impl<M: BackupMedium> ObjectSource for BackupContent<'_, M> {
        fn describe(
            &self,
            domain: ContentDomainId,
            root: ContentHash,
        ) -> Result<TransferManifest, ContentError> {
            let bytes = self
                .medium
                .read(&self.root.join(format!("{root}.manifest")), 1024 * 1024)
                .map_err(ContentError::Io)?;
            describe_encoded(domain, root, bytes)
        }
        fn chunk(&self, transfer: &TransferManifest, index: usize) -> Result<Vec<u8>, ContentError> {
            let (hash, length) = transfer.chunk(index)?;
            let bytes = self
                .medium
                .read(&self.root.join(format!("{hash}.chunk")), length as usize)
                .map_err(ContentError::Io)?;
            if bytes.len() != length as usize
                || ContentHash(*blake3::hash(&bytes).as_bytes()) != hash
            {
                return Err(ContentError::Corrupt);
            }
            Ok(bytes)
        }
    }

    /// The decoder pair a native session's log promises: the managed
    /// predecessor and the native successor this binary compiles.
    pub fn decoder_pair() -> ([u8; 32], [u8; 32]) {
        (managed_format_hash(), native_format_hash())
    }
    /// The exact bytes and coordinates a replica exported (`checkpoint_evidence`).
    pub struct BackupImage {
        pub checkpoint: Vec<u8>,
        pub prefix: EvidencePrefix,
        pub configuration: MembershipConfiguration,
    }
    /// What the envelope names, derived from its bytes alone.
    pub struct Inventory {
        pub header: crate::native_checkpoint::Header,
        pub profile: NativeContentProfile,
        pub seeds: Vec<SeedChunk>,
        /// Every content root the rows and the bundle headers name, sorted.
        pub objects: Vec<ContentHash>,
        pub retention: Option<RetentionSection>,
        pub bundles: u64,
    }
    /// A session envelope decoded and its native Core rebuilt against a
    /// content source: what a backup, a verification and a restore all
    /// start from.
    pub struct DecodedImage {
        envelope: SnapshotEnvelopeV7,
        pub profile: NativeContentProfile,
        pub header: crate::native_checkpoint::Header,
        pub retention: Option<RetentionSection>,
        pub seeds: Vec<SeedChunk>,
        pub core: Core<NativeState>,
    }
    /// Decode a session envelope, assemble a seeded root from `seeds`, and
    /// rebuild the native Core through the recovery path every replica
    /// uses, which reads and verifies every live artifact's object from
    /// `source` on the way.
    pub fn decode_image<S: ObjectSource>(
        checkpoint: &[u8],
        seeds: &impl SeedSource,
        source: &S,
        limits: &NativeSessionLimits,
        budget: &MemoryBudget,
    ) -> Result<DecodedImage, BackupError> {
        let data = checkpoint
            .strip_prefix(SNAPSHOT_V7_MAGIC)
            .ok_or(BackupError::Unsupported)?;
        let _decode = budget.reserve(
            BudgetKind::Recovery,
            BudgetLane::Completion,
            checkpoint
                .len()
                .checked_mul(4)
                .and_then(|n| n.checked_add(4096))
                .ok_or(BackupError::Capacity)?,
        )?;
        let (envelope, rest): (SnapshotEnvelopeV7, _) = durable_session_v1::take(data)?;
        if !rest.is_empty() {
            return Err(BackupError::Corrupt("session envelope"));
        }
        let activation = ActivationRecord::decode(&envelope.activation)?;
        let native = envelope.native.as_slice();
        let assembled = match Checkpoint::describe(native, limits.checkpoint)? {
            None => None,
            Some(manifest) => {
                let mut table = Vec::new();
                for chunk in manifest.chunks() {
                    let chunk = chunk?;
                    table.try_reserve_exact(1).map_err(|_| BackupError::Capacity)?;
                    table.push(chunk);
                }
                let core = manifest
                    .assemble_with(|hash, length| seeds.read_seed(hash, length), budget)
                    .map_err(|error| match error {
                    crate::native_checkpoint::SeedError::Missing(_) => {
                        BackupError::Missing("seed chunk")
                    }
                    crate::native_checkpoint::SeedError::Memory(error) => BackupError::Memory(error),
                    crate::native_checkpoint::SeedError::Seeds(error) => BackupError::Content(error),
                    crate::native_checkpoint::SeedError::Invalid(_) => {
                        BackupError::Corrupt("seeded root")
                    }
                })?;
                Some((table, core))
            }
        };
        let (seeds, checkpoint) = match &assembled {
            Some((table, core)) => (
                table.clone(),
                Checkpoint::inspect_seeded(native, core.bytes(), limits.checkpoint)?,
            ),
            None => (Vec::new(), Checkpoint::inspect(native, limits.checkpoint)?),
        };
        let header = checkpoint.header();
        if header.ledger != envelope.state.state.state.ledger
            || header.metadata.applied_raft != envelope.state.state.state.raft_index
        {
            return Err(BackupError::Corrupt("envelope coordinates"));
        }
        let core = recovery::restore(
            checkpoint.core(),
            RangeId(1),
            limits.recovery,
            budget.clone(),
            source,
            &BuiltinNativeSchemas,
        )?;
        if core.native_sequence() != header.prefix {
            return Err(BackupError::Corrupt("restored prefix"));
        }
        let retention = checkpoint.retention();
        Ok(DecodedImage {
            envelope,
            profile: activation.profile,
            header,
            retention,
            seeds,
            core,
        })
    }
    /// Decode a session envelope, rebuild its native Core against `source`
    /// and walk every content root it names (26 §5): the objects of live
    /// artifacts, the archive bundles of retired families and everything a
    /// bundle's header names. `seeds` supplies the chunks of a seeded root.
    pub fn inventory<S: ObjectSource>(
        checkpoint: &[u8],
        seeds: &impl SeedSource,
        source: &S,
        limits: &NativeSessionLimits,
        budget: &MemoryBudget,
    ) -> Result<Inventory, BackupError> {
        let DecodedImage {
            profile,
            header,
            retention,
            seeds,
            core,
            ..
        } = decode_image(checkpoint, seeds, source, limits, budget)?;
        let domain = limits.content_domain;
        let mut objects = BTreeSet::new();
        let mut bundles = Vec::new();
        let mut cursor = None;
        loop {
            let page = core.native_content_roots(cursor, PAGE)?;
            for root in page.roots {
                match root {
                    ContentRoot::Artifact { pointer, .. } | ContentRoot::Inline { pointer, .. } => {
                        if pointer.domain != domain {
                            return Err(BackupError::Corrupt("artifact domain"));
                        }
                        objects.insert(pointer.root);
                    }
                    ContentRoot::Bundle { root, bytes, .. } => {
                        objects.insert(root);
                        bundles.try_reserve_exact(1).map_err(|_| BackupError::Capacity)?;
                        bundles.push((root, bytes));
                    }
                }
            }
            cursor = page.next;
            if cursor.is_none() {
                break;
            }
        }
        for (root, bytes) in &bundles {
            let reference = ContentRef {
                domain,
                root: *root,
                length: *bytes,
                class: ContentClass::Evidence,
            };
            let read = source.read_content(&reference, MAX_BUNDLE_BYTES)?;
            let archive = StructuralArchive::inspect(&read, limits.inspection)?;
            let inner = archive.header();
            if inner.ledger != header.ledger {
                return Err(BackupError::Corrupt("bundle ledger"));
            }
            for root in inner.content.iter().chain(&inner.inline) {
                objects.insert(*root);
            }
        }
        let mut sorted = Vec::new();
        sorted
            .try_reserve_exact(objects.len())
            .map_err(|_| BackupError::Capacity)?;
        sorted.extend(objects);
        Ok(Inventory {
            header,
            profile,
            seeds,
            objects: sorted,
            retention,
            bundles: u64::try_from(bundles.len()).map_err(|_| BackupError::Capacity)?,
        })
    }

    /// The identity a restored session takes (26 §6): the cluster and log
    /// group it will live in, and the node that founds its log alone.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct Incarnation {
        pub cluster: [u8; 16],
        pub group: [u8; 16],
        pub node: u64,
    }
    /// The envelope rewritten for an incarnation, and what a log begins
    /// with to install it.
    pub struct RewrittenImage {
        pub bytes: Vec<u8>,
        pub index: u64,
        pub term: u64,
        pub native_sequence: u64,
        /// The activation genesis the rewritten envelope carries.
        pub genesis: ContentHash,
        pub configuration: MembershipConfiguration,
    }
    /// Rewrite a backup's envelope for `target`: the same rows, cursors,
    /// request streams and activation, under the target's cluster and log
    /// group with the activation genesis re-derived for them, a bootstrap
    /// membership of the founding node alone, no placement and no
    /// membership receipt (the restored session registers with its
    /// directory as a session founded here, at route epoch 1), and the
    /// movement section dropped (the layout is rebuilt from the rows). A
    /// seeded root's chunks are sealed into `sink`, the seed store the
    /// restored session will open.
    #[allow(
        clippy::too_many_arguments,
        reason = "one bounded pass over borrowed sources; nothing is retained"
    )]
    pub fn rewrite<S: ObjectSource>(
        checkpoint: &[u8],
        seeds: &impl SeedSource,
        source: &S,
        target: Incarnation,
        limits: &NativeSessionLimits,
        budget: &MemoryBudget,
        sink: &mut focal_evidence::SeedStore,
    ) -> Result<RewrittenImage, BackupError> {
        if target.node == 0 || target.group == [0; 16] {
            return Err(BackupError::Corrupt("incarnation"));
        }
        let decoded = decode_image(checkpoint, seeds, source, limits, budget)?;
        let meta = decoded.header.metadata;
        let genesis = crate::native_checkpoint::genesis(
            target.cluster,
            target.group,
            decoded.header.ledger,
            decoded.profile,
            meta.activation.decoder,
        );
        let metadata = crate::native_checkpoint::Metadata {
            cluster: target.cluster,
            group: target.group,
            activation: crate::native_checkpoint::Activation {
                genesis,
                ..meta.activation
            },
            ..meta
        };
        let configuration = MembershipConfiguration {
            voters: vec![target.node],
            ..MembershipConfiguration::default()
        };
        let plan = crate::native_checkpoint::EncodingPlan::prepare_with_sections(
            &decoded.core,
            metadata,
            &configuration,
            None,
            decoded.retention,
            limits.checkpoint,
        )?;
        let encoded = plan.encode_in_seeded(budget, sink)?;
        let (native, _allocation) = encoded.into_parts();
        let mut envelope = decoded.envelope;
        envelope.native = native;
        envelope.state.state.membership = MembershipState::default();
        envelope.state.placement = PlacementState::default();
        let bytes = durable_session_v1::encode_view(
            SNAPSHOT_V7_MAGIC,
            &focal_model::durable_v1::Ref(&envelope),
            MAX_CHECKPOINT_BYTES,
        )?;
        Ok(RewrittenImage {
            bytes,
            index: meta.applied_raft,
            term: meta.applied_term,
            native_sequence: decoded.header.prefix.0,
            genesis,
            configuration,
        })
    }

    /// Import every object a backup lists into `store`, chunk by chunk,
    /// exactly as a custody transfer installs them (each chunk verified,
    /// the manifest published only once every chunk is local). Objects the
    /// store already holds are verified in place.
    pub fn import_content<M: BackupMedium>(
        medium: &M,
        root: &Path,
        manifest: &BackupManifest,
        store: &mut focal_evidence::ContentStore,
        budget: &MemoryBudget,
    ) -> Result<u64, BackupError> {
        let content = BackupContent::new(medium, root);
        let mut imported = 0u64;
        for object in &manifest.content {
            let reference = ContentRef {
                domain: manifest.domain,
                root: object.root,
                length: object.length,
                class: object.class,
            };
            let encoded = medium
                .read(
                    &root.join(CONTENT_DIR).join(format!("{}.manifest", object.root)),
                    1024 * 1024,
                )
                .map_err(ContentError::Io)?;
            let _scratch = budget.reserve(
                BudgetKind::Recovery,
                BudgetLane::Completion,
                encoded
                    .len()
                    .checked_mul(4)
                    .and_then(|n| n.checked_add(4096))
                    .ok_or(BackupError::Capacity)?,
            )?;
            let transfer = store.prepare_import(reference, encoded)?;
            for index in 0..transfer.chunks() {
                let (_, length) = transfer.chunk(index)?;
                let _chunk = budget.reserve(
                    BudgetKind::Recovery,
                    BudgetLane::Completion,
                    (length as usize)
                        .checked_mul(2)
                        .ok_or(BackupError::Capacity)?,
                )?;
                let bytes = content.chunk(&transfer, index)?;
                store.import_chunk(&transfer, index, &bytes)?;
            }
            let installed = store.complete_import(&transfer)?;
            if installed != *transfer.reference() {
                return Err(BackupError::Corrupt("imported object"));
            }
            imported = imported.saturating_add(1);
        }
        Ok(imported)
    }
    /// Install every seed chunk a backup lists into `sink`.
    pub fn import_seeds<M: BackupMedium>(
        medium: &M,
        root: &Path,
        manifest: &BackupManifest,
        sink: &mut focal_evidence::SeedStore,
    ) -> Result<u64, BackupError> {
        let seeds = BackupSeeds::new(medium, root);
        let mut installed = 0u64;
        for chunk in &manifest.seeds {
            let bytes = seeds.read_seed(chunk.hash, chunk.length as usize)?;
            sink.install_as(chunk.hash, &bytes)?;
            installed = installed.saturating_add(1);
        }
        Ok(installed)
    }

    /// What `write` produced.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct BackupReport {
        pub manifest: BackupManifest,
        pub files: u64,
        pub bytes: u64,
        pub bundles: u64,
    }
    /// Write a backup of `image` under `root`: the envelope, the seed chunks
    /// it names, the exact tree of every object its prefix names, and the
    /// manifest last. Refuses a directory that already holds a manifest.
    #[allow(
        clippy::too_many_arguments,
        reason = "one bounded pass over borrowed sources; nothing is retained"
    )]
    pub fn write<M: BackupMedium, S: ObjectSource>(
        medium: &mut M,
        root: &Path,
        image: &BackupImage,
        seeds: &impl SeedSource,
        source: &S,
        decoder: ([u8; 32], [u8; 32]),
        limits: &NativeSessionLimits,
        budget: &MemoryBudget,
        now_ms: u64,
    ) -> Result<BackupReport, BackupError> {
        if image.checkpoint.is_empty()
            || image.checkpoint.len() > MAX_CHECKPOINT_BYTES
            || ContentHash(*blake3::hash(&image.checkpoint).as_bytes()) != image.prefix.checkpoint
            || image.prefix.checkpoint_bytes != image.checkpoint.len() as u64
        {
            return Err(BackupError::Corrupt("image"));
        }
        if medium.exists(&root.join(MANIFEST_FILE)) {
            return Err(BackupError::Exists);
        }
        let inventory = inventory(&image.checkpoint, seeds, source, limits, budget)?;
        let header = inventory.header;
        if header.ledger != image.prefix.ledger
            || header.metadata.applied_raft != image.prefix.index.0
            || header.metadata.applied_term != image.prefix.term.0
            || header.metadata.cluster != image.prefix.cluster
            || header.metadata.group != image.prefix.group.0
        {
            return Err(BackupError::Corrupt("image coordinates"));
        }
        medium.create_dir(root)?;
        medium.create_dir(&root.join(SEEDS_DIR))?;
        medium.create_dir(&root.join(CONTENT_DIR))?;
        install(medium, &root.join(CHECKPOINT_FILE), &image.checkpoint)?;
        let mut files = 1u64;
        let mut bytes = image.checkpoint.len() as u64;
        let mut seed_entries = Vec::new();
        seed_entries
            .try_reserve_exact(inventory.seeds.len())
            .map_err(|_| BackupError::Capacity)?;
        for chunk in &inventory.seeds {
            let length = usize::try_from(chunk.length).map_err(|_| BackupError::Capacity)?;
            let _scratch = budget.reserve(BudgetKind::Recovery, BudgetLane::Completion, length)?;
            let read = seeds.read_seed(chunk.hash, length)?;
            if read.len() != length || ContentHash(*blake3::hash(&read).as_bytes()) != chunk.hash {
                return Err(BackupError::Corrupt("seed chunk"));
            }
            install(medium, &root.join(SEEDS_DIR).join(format!("{}.seed", chunk.hash)), &read)?;
            files = files.saturating_add(1);
            bytes = bytes.saturating_add(u64::from(chunk.length));
            seed_entries.push(BackupChunk {
                hash: chunk.hash,
                length: chunk.length,
            });
        }
        let domain = limits.content_domain;
        let mut content = Vec::new();
        content
            .try_reserve_exact(inventory.objects.len())
            .map_err(|_| BackupError::Capacity)?;
        let mut written = BTreeSet::new();
        for object in &inventory.objects {
            let transfer = source.describe(domain, *object)?;
            let _scratch = budget.reserve(
                BudgetKind::Recovery,
                BudgetLane::Completion,
                transfer.resident_bytes()?,
            )?;
            install(
                medium,
                &root.join(CONTENT_DIR).join(format!("{object}.manifest")),
                transfer.encoded(),
            )?;
            files = files.saturating_add(1);
            let mut chunks = Vec::new();
            chunks
                .try_reserve_exact(transfer.chunks())
                .map_err(|_| BackupError::Capacity)?;
            for index in 0..transfer.chunks() {
                let (hash, length) = transfer.chunk(index)?;
                if written.insert(hash) {
                    let _chunk = budget.reserve(
                        BudgetKind::Recovery,
                        BudgetLane::Completion,
                        length as usize,
                    )?;
                    let read = source.chunk(&transfer, index)?;
                    install(medium, &root.join(CONTENT_DIR).join(format!("{hash}.chunk")), &read)?;
                    files = files.saturating_add(1);
                    bytes = bytes.saturating_add(u64::from(length));
                }
                chunks.push(BackupChunk { hash, length });
            }
            content.push(BackupObject {
                root: *object,
                length: transfer.reference().length,
                class: transfer.reference().class,
                chunks,
            });
        }
        let manifest = BackupManifest {
            schema: SCHEMA,
            created_ms: now_ms,
            prefix: image.prefix.clone(),
            native_sequence: header.prefix.0,
            native_genesis: header.metadata.activation.genesis,
            profile: match inventory.profile {
                NativeContentProfile::ProjectionOnly => 0,
                NativeContentProfile::AuthoredV1 => 1,
            },
            domain: limits.content_domain,
            decoder_predecessor: decoder.0,
            decoder_successor: decoder.1,
            configuration: image.configuration.clone(),
            checkpoint_hash: image.prefix.checkpoint,
            checkpoint_bytes: image.prefix.checkpoint_bytes,
            seeds: seed_entries,
            content,
            archived_through: inventory
                .retention
                .map_or(0, |section| section.archived_through.0),
            retired_families: inventory
                .retention
                .map_or(0, |section| section.retired_families),
        };
        manifest.validate()?;
        install(medium, &root.join(MANIFEST_FILE), &manifest.encode()?)?;
        files = files.saturating_add(1);
        Ok(BackupReport {
            manifest,
            files,
            bytes,
            bundles: inventory.bundles,
        })
    }

    /// What `verify` found.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct VerifyReport {
        pub manifest: BackupManifest,
        pub checkpoint_verified: bool,
        pub seeds_verified: u64,
        pub objects_verified: u64,
        pub chunks_verified: u64,
        pub bytes_verified: u64,
        /// The envelope decodes and rebuilds, and names exactly the objects
        /// and seeds the manifest lists.
        pub inventory_matches: bool,
        /// This binary carries the decoder the backup's log promised.
        pub decoder_supported: bool,
        pub problems: Vec<String>,
    }
    impl VerifyReport {
        pub fn complete(&self) -> bool {
            self.problems.is_empty() && self.inventory_matches && self.checkpoint_verified
        }
        fn note(&mut self, problem: String) {
            if self.problems.len() < MAX_PROBLEMS && self.problems.try_reserve(1).is_ok() {
                self.problems.push(problem);
            }
        }
    }
    /// Verify a backup under `root` file by file, then rebuild its envelope
    /// against its own content files and compare what it names with what
    /// the manifest lists. Nothing is written.
    pub fn verify<M: BackupMedium>(
        medium: &M,
        root: &Path,
        native_decoder: [u8; 32],
        budget: &MemoryBudget,
    ) -> Result<VerifyReport, BackupError> {
        let manifest = BackupManifest::decode(
            &medium
                .read(&root.join(MANIFEST_FILE), MAX_MANIFEST_BYTES)
                .map_err(|_| BackupError::Missing("manifest"))?,
        )?;
        // The backup names its own domain: verification is possible on any
        // machine that carries the decoder, not only the node that wrote it.
        let limits = NativeSessionLimits::standard(manifest.domain);
        let limits = &limits;
        let mut report = VerifyReport {
            checkpoint_verified: false,
            seeds_verified: 0,
            objects_verified: 0,
            chunks_verified: 0,
            bytes_verified: 0,
            inventory_matches: false,
            decoder_supported: manifest.decoder_successor == native_decoder,
            problems: Vec::new(),
            manifest,
        };
        let manifest = &report.manifest;
        let checkpoint = match medium.read(&root.join(CHECKPOINT_FILE), MAX_CHECKPOINT_BYTES) {
            Ok(bytes) => bytes,
            Err(error) => {
                let mut report = report;
                report.note(format!("checkpoint: {error}"));
                return Ok(report);
            }
        };
        let checkpoint_ok = checkpoint.len() as u64 == manifest.checkpoint_bytes
            && ContentHash(*blake3::hash(&checkpoint).as_bytes()) == manifest.checkpoint_hash;
        let mut problems = Vec::new();
        if !checkpoint_ok {
            problems.push("checkpoint: hash or length differs".to_owned());
        }
        let mut seeds_verified = 0u64;
        for chunk in &manifest.seeds {
            match medium.read(
                &root.join(SEEDS_DIR).join(format!("{}.seed", chunk.hash)),
                chunk.length as usize,
            ) {
                Ok(bytes)
                    if bytes.len() == chunk.length as usize
                        && ContentHash(*blake3::hash(&bytes).as_bytes()) == chunk.hash =>
                {
                    seeds_verified = seeds_verified.saturating_add(1);
                }
                Ok(_) => problems.push(format!("seed {}: hash or length differs", chunk.hash)),
                Err(error) => problems.push(format!("seed {}: {error}", chunk.hash)),
            }
        }
        let domain = manifest.domain;
        let content = BackupContent::new(medium, root);
        let mut objects_verified = 0u64;
        let mut chunks_verified = 0u64;
        let mut bytes_verified = 0u64;
        let mut seen = BTreeSet::new();
        for object in &manifest.content {
            let transfer = match content.describe(domain, object.root) {
                Ok(transfer) => transfer,
                Err(error) => {
                    problems.push(format!("object {}: {error}", object.root));
                    continue;
                }
            };
            let listed = (0..transfer.chunks())
                .map(|index| transfer.chunk(index))
                .collect::<Result<Vec<_>, _>>()
                .unwrap_or_default();
            if transfer.reference().length != object.length
                || transfer.reference().class != object.class
                || listed.len() != object.chunks.len()
                || listed
                    .iter()
                    .zip(&object.chunks)
                    .any(|((hash, length), chunk)| *hash != chunk.hash || *length != chunk.length)
            {
                problems.push(format!("object {}: manifest differs from the listing", object.root));
                continue;
            }
            let mut whole = true;
            for index in 0..transfer.chunks() {
                let (hash, length) = match transfer.chunk(index) {
                    Ok(chunk) => chunk,
                    Err(_) => {
                        whole = false;
                        break;
                    }
                };
                match content.chunk(&transfer, index) {
                    Ok(_) => {
                        if seen.insert(hash) {
                            chunks_verified = chunks_verified.saturating_add(1);
                            bytes_verified = bytes_verified.saturating_add(u64::from(length));
                        }
                    }
                    Err(error) => {
                        problems.push(format!("chunk {hash}: {error}"));
                        whole = false;
                    }
                }
            }
            if whole {
                objects_verified = objects_verified.saturating_add(1);
            }
        }
        let inventory_matches = if checkpoint_ok && problems.is_empty() {
            let seeds = BackupSeeds::new(medium, root);
            {
                match inventory(&checkpoint, &seeds, &content, limits, budget) {
                    Ok(inventory) => {
                        let listed: Vec<ContentHash> =
                            manifest.content.iter().map(|object| object.root).collect();
                        let seeds_match = inventory.seeds.len() == manifest.seeds.len()
                            && inventory
                                .seeds
                                .iter()
                                .zip(&manifest.seeds)
                                .all(|(a, b)| a.hash == b.hash && a.length == b.length);
                        let coordinates = inventory.header.ledger == manifest.prefix.ledger
                            && inventory.header.prefix.0 == manifest.native_sequence
                            && inventory.header.metadata.applied_raft == manifest.prefix.index.0
                            && inventory.header.metadata.applied_term == manifest.prefix.term.0
                            && inventory.header.metadata.activation.genesis
                                == manifest.native_genesis;
                        if inventory.objects != listed {
                            problems.push("inventory: the envelope names objects the manifest does not list, or the reverse".to_owned());
                        }
                        if !seeds_match {
                            problems.push("inventory: seed chunks differ from the manifest".to_owned());
                        }
                        if !coordinates {
                            problems.push("inventory: envelope coordinates differ from the manifest".to_owned());
                        }
                        inventory.objects == listed && seeds_match && coordinates
                    }
                    Err(error) => {
                        problems.push(format!("inventory: {error}"));
                        false
                    }
                }
            }
        } else {
            false
        };
        report.checkpoint_verified = checkpoint_ok;
        report.seeds_verified = seeds_verified;
        report.objects_verified = objects_verified;
        report.chunks_verified = chunks_verified;
        report.bytes_verified = bytes_verified;
        report.inventory_matches = inventory_matches;
        for problem in problems {
            report.note(problem);
        }
        Ok(report)
    }
}
