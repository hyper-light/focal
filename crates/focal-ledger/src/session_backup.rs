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
        BuiltinNativeSchemas, ColdGeometry, ColdReader, ColdState, ColdStreams, ContentError,
        ContentReader, MAX_TRANSFER_MANIFEST_BYTES, NativeCustodyReader, SeedReader,
        TransferManifest, describe_encoded,
    };
    use focal_memory::RangeId;
    use hyper_seal::keys::{KeyId, RECORD, Wrapped, WrappingKey};
    use hyper_seal::name::Namer;
    use hyper_seal::{SealError, Secret32};
    use std::collections::BTreeSet;
    use std::path::{Path, PathBuf};

    /// `FCLBKUP1`: the manifest's magic; a schema, a postcard body and a
    /// BLAKE3 trailer over both follow.
    pub const MAGIC: &[u8; 8] = b"FCLBKUP1";
    /// Schema 2 carries the session's cold streams (doc 31 M2, A2); schema 1, written before
    /// them, is read as one with none.
    pub const SCHEMA: u16 = 2;
    const SCHEMA_V1: u16 = 1;
    pub const MANIFEST_FILE: &str = "MANIFEST";
    pub const CHECKPOINT_FILE: &str = "checkpoint";
    pub const SEEDS_DIR: &str = "seeds";
    pub const CONTENT_DIR: &str = "content";
    /// The chunks of the session's cold streams (doc 31 M2, A2): `cold/data` and `cold/marks`.
    pub const COLD_DIR: &str = "cold";
    fn cold_part(data: bool) -> &'static str {
        if data { "data" } else { "marks" }
    }
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
        /// A sealed backup asked of with no key file (29 §12.8): `key` is the ID of the operator
        /// key it needs, hex.
        #[error("the backup is sealed under key {key}: name its key file")]
        KeyRequired { key: String },
        /// A sealed backup asked of with a key file that does not wrap its key: the key it needs
        /// and the key the file holds, by their IDs, hex.
        #[error("the backup is sealed under key {needed}; the key file holds key {offered}")]
        WrongKey { needed: String, offered: String },
        /// The operator's key file could not be read as a key.
        #[error("key file: {0}")]
        KeyFile(#[from] focal_seal::SealSetupError),
        /// A sealed backup's key could not be made or used.
        #[error("the backup's key: {0}")]
        Seal(#[from] SealError),
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
        /// Every chunk of the session's cold streams the envelope names (doc 31 M2, A2), the data
        /// stream's and the marks', manifest chunks among them: empty without cold streams.
        pub cold_data: Vec<BackupChunk>,
        pub cold_marks: Vec<BackupChunk>,
    }
    /// A manifest of schema 1, written before cold streams: read as one naming none.
    #[derive(Deserialize)]
    struct BackupManifestV1 {
        schema: u16,
        created_ms: u64,
        prefix: EvidencePrefix,
        native_sequence: u64,
        native_genesis: ContentHash,
        profile: u8,
        domain: ContentDomainId,
        decoder_predecessor: [u8; 32],
        decoder_successor: [u8; 32],
        configuration: MembershipConfiguration,
        checkpoint_hash: ContentHash,
        checkpoint_bytes: u64,
        seeds: Vec<BackupChunk>,
        content: Vec<BackupObject>,
        archived_through: u64,
        retired_families: u64,
    }
    impl From<BackupManifestV1> for BackupManifest {
        fn from(old: BackupManifestV1) -> Self {
            Self {
                schema: old.schema,
                created_ms: old.created_ms,
                prefix: old.prefix,
                native_sequence: old.native_sequence,
                native_genesis: old.native_genesis,
                profile: old.profile,
                domain: old.domain,
                decoder_predecessor: old.decoder_predecessor,
                decoder_successor: old.decoder_successor,
                configuration: old.configuration,
                checkpoint_hash: old.checkpoint_hash,
                checkpoint_bytes: old.checkpoint_bytes,
                seeds: old.seeds,
                content: old.content,
                archived_through: old.archived_through,
                retired_families: old.retired_families,
                cold_data: Vec::new(),
                cold_marks: Vec::new(),
            }
        }
    }
    impl BackupManifest {
        /// The heap and value storage the decoded manifest holds — its
        /// objects and their chunk lists, its seeds, its membership — which a
        /// holder charges for as long as it keeps it (the audit's F51).
        pub fn resident_bytes(&self) -> Option<usize> {
            let chunk = std::mem::size_of::<BackupChunk>();
            let objects = self.content.iter().try_fold(
                self.content
                    .capacity()
                    .checked_mul(std::mem::size_of::<BackupObject>())?,
                |sum, object| sum.checked_add(object.chunks.capacity().checked_mul(chunk)?),
            )?;
            let configuration = [
                &self.configuration.voters,
                &self.configuration.learners,
                &self.configuration.voters_outgoing,
                &self.configuration.learners_next,
            ]
            .iter()
            .try_fold(0usize, |sum, ids| {
                sum.checked_add(ids.capacity().checked_mul(std::mem::size_of::<u64>())?)
            })?;
            std::mem::size_of::<Self>()
                .checked_add(self.seeds.capacity().checked_mul(chunk)?)?
                .checked_add(self.cold_data.capacity().checked_mul(chunk)?)?
                .checked_add(self.cold_marks.capacity().checked_mul(chunk)?)?
                .checked_add(objects)?
                .checked_add(configuration)
        }
        pub fn encode(&self) -> Result<Vec<u8>, BackupError> {
            // Only the current schema is written: an older one is read, never made.
            if self.schema != SCHEMA {
                return Err(BackupError::Corrupt("manifest schema"));
            }
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
            let (schema, _): (u16, &[u8]) =
                postcard::take_from_bytes(body).map_err(|_| BackupError::Corrupt("manifest body"))?;
            let (manifest, rest): (Self, &[u8]) = match schema {
                SCHEMA => postcard::take_from_bytes(body)
                    .map_err(|_| BackupError::Corrupt("manifest body"))?,
                SCHEMA_V1 => {
                    let (old, rest): (BackupManifestV1, &[u8]) = postcard::take_from_bytes(body)
                        .map_err(|_| BackupError::Corrupt("manifest body"))?;
                    (Self::from(old), rest)
                }
                _ => return Err(BackupError::Corrupt("manifest schema")),
            };
            if !rest.is_empty() {
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
                || self
                    .cold_data
                    .iter()
                    .chain(&self.cold_marks)
                    .any(|chunk| chunk.length == 0)
            {
                return Err(BackupError::Corrupt("manifest fields"));
            }
            Ok(())
        }
        /// Files and bytes the backup holds besides its manifest.
        pub fn totals(&self) -> (u64, u64) {
            let mut files = 1u64;
            let mut bytes = self.checkpoint_bytes;
            for chunk in self.seeds.iter().chain(&self.cold_data).chain(&self.cold_marks) {
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

    /// Where a backup's files go and how they become durable: focal-platform's
    /// medium, under the name the backup suites use.
    pub use focal_platform::fs::{FileMedium, Medium as BackupMedium};
    /// Install one file durably (`focal_platform::fs::install`); a path with no
    /// parent directory is a corrupt backup path.
    fn install<M: BackupMedium>(medium: &mut M, path: &Path, bytes: &[u8]) -> Result<(), BackupError> {
        if path.parent().filter(|p| !p.as_os_str().is_empty()).is_none() {
            return Err(BackupError::Corrupt("backup path"));
        }
        focal_platform::fs::install(medium, path, bytes)?;
        Ok(())
    }

    /// A sealed backup's key, wrapped under the operator's key file, kept in the clear beside the
    /// files it seals (29 §12.8), as CockroachDB keeps a backup's `ENCRYPTION-INFO`: its presence
    /// says the backup is sealed, and its record says which key file opens it.
    pub const ENCRYPTION_INFO_FILE: &str = "ENCRYPTION-INFO";
    /// `FCLBKEY1`: the encryption record's magic; the backup key's ID, its record under the
    /// operator's key and a BLAKE3 trailer over the three follow.
    const KEY_MAGIC: &[u8; 8] = b"FCLBKEY1";
    /// The trailer's bytes: a BLAKE3 hash, which tells a damaged record from another key's.
    const KEY_TRAILER: usize = 32;
    const KEY_ID: usize = 16;
    /// Bytes of a backup's `ENCRYPTION-INFO`.
    const KEY_INFO: usize = KEY_MAGIC.len() + KEY_ID + RECORD + KEY_TRAILER;
    /// What a sealed backup's keyed names are derived under, as a content domain's are (29 §11).
    const NAMES: &[u8] = b"focal backup names";

    /// How a backup's files lie (29 §12.8): in the clear, as every backup below the at-rest level
    /// is written and every backup before it was; or sealed, each file whole under a data key of
    /// its own wrapped by a key made for the backup (`focal_seal::file`), and each file a hash
    /// names filed under the backup's keyed name of it, so no file holds a row, a chunk, a hash or
    /// the manifest in the clear.
    pub struct BackupForm {
        sealed: Option<SealedBackup>,
    }
    struct SealedBackup {
        key: WrappingKey,
        names: Namer,
        /// The ID of the operator key the backup's key is wrapped under.
        operator: KeyId,
        /// The backup's `ENCRYPTION-INFO`: installed before any file it seals.
        info: [u8; KEY_INFO],
    }
    impl BackupForm {
        /// A backup in the clear.
        pub fn clear() -> Self {
            Self { sealed: None }
        }
        /// A new sealed backup's form: a random key made for it and recorded under `operator`, the
        /// operator's key file ([`focal_seal::operator_key`]).
        pub fn seal_under(operator: &WrappingKey) -> Result<Self, BackupError> {
            let (secret, record) = operator.make_child()?;
            let id = KeyId::random()?;
            let mut info = [0u8; KEY_INFO];
            let mut at = 0usize;
            for field in [KEY_MAGIC.as_slice(), id.0.as_slice(), record.encode().as_slice()] {
                let end = at.checked_add(field.len()).ok_or(BackupError::Capacity)?;
                info.get_mut(at..end)
                    .ok_or(BackupError::Capacity)?
                    .copy_from_slice(field);
                at = end;
            }
            let (body, trailer) = info
                .split_at_mut_checked(at)
                .ok_or(BackupError::Capacity)?;
            trailer.copy_from_slice(blake3::hash(body).as_bytes());
            Self::from_parts(id, secret, operator.id(), info)
        }
        /// The form of the backup under `root`, read through `medium`: in the clear where it holds
        /// no `ENCRYPTION-INFO`; sealed, its key unwrapped under `operator`. A sealed backup asked
        /// of with no key file is [`BackupError::KeyRequired`], with a key file that does not wrap
        /// its key [`BackupError::WrongKey`]; a record that does not parse, or fails its trailer,
        /// is corrupt. Nothing else of the backup is read.
        pub fn of<M: BackupMedium>(
            medium: &M,
            root: &Path,
            operator: Option<&WrappingKey>,
        ) -> Result<Self, BackupError> {
            let path = root.join(ENCRYPTION_INFO_FILE);
            if !medium.exists(&path)? {
                return Ok(Self::clear());
            }
            let bytes = medium.read(&path, KEY_INFO)?;
            let info = <[u8; KEY_INFO]>::try_from(bytes.as_slice())
                .map_err(|_| BackupError::Corrupt("encryption info"))?;
            let body = info
                .get(..KEY_INFO.saturating_sub(KEY_TRAILER))
                .ok_or(BackupError::Corrupt("encryption info"))?;
            let trailer = info
                .get(body.len()..)
                .ok_or(BackupError::Corrupt("encryption info"))?;
            if !body.starts_with(KEY_MAGIC) || blake3::hash(body).as_bytes() != trailer {
                return Err(BackupError::Corrupt("encryption info"));
            }
            let id_end = KEY_MAGIC.len().saturating_add(KEY_ID);
            let id = body
                .get(KEY_MAGIC.len()..id_end)
                .and_then(|id| <[u8; KEY_ID]>::try_from(id).ok())
                .map(KeyId)
                .ok_or(BackupError::Corrupt("encryption info"))?;
            let record = body
                .get(id_end..)
                .ok_or(BackupError::Corrupt("encryption info"))
                .and_then(|record| {
                    Wrapped::decode(record).map_err(|_| BackupError::Corrupt("encryption info"))
                })?;
            let operator = operator.ok_or_else(|| BackupError::KeyRequired {
                key: hex_id(&record.parent),
            })?;
            if record.parent != operator.id() {
                return Err(BackupError::WrongKey {
                    needed: hex_id(&record.parent),
                    offered: hex_id(&operator.id()),
                });
            }
            // The record names this key and its trailer holds, yet it does not unwrap: changed by
            // someone who could recompute the trailer, which no key file's choice explains.
            let secret = operator
                .unwrap(&record)
                .map_err(|_| BackupError::Corrupt("encryption info"))?;
            Self::from_parts(id, secret, record.parent, info)
        }
        fn from_parts(
            id: KeyId,
            secret: Secret32,
            operator: KeyId,
            info: [u8; KEY_INFO],
        ) -> Result<Self, BackupError> {
            let key = WrappingKey::new(id, 0, secret);
            let (naming, _) = key.derive_child(NAMES, &id.0)?;
            let names = Namer::new(&naming)?;
            Ok(Self {
                sealed: Some(SealedBackup {
                    key,
                    names,
                    operator,
                    info,
                }),
            })
        }
        /// Whether the backup's files are sealed.
        pub fn sealed(&self) -> bool {
            self.sealed.is_some()
        }
        /// The ID of the operator key a sealed backup needs, hex: what its `ENCRYPTION-INFO`
        /// records, so an operator knows which key file opens it.
        pub fn key(&self) -> Option<String> {
            self.sealed.as_ref().map(|sealed| hex_id(&sealed.operator))
        }
        /// The name of the file of `hash` with `extension`: its hash in the clear, the backup's
        /// keyed name of it sealed.
        fn name(&self, hash: &ContentHash, extension: &str) -> Result<String, BackupError> {
            use std::fmt::Write as _;
            let Some(sealed) = &self.sealed else {
                return Ok(format!("{hash}.{extension}"));
            };
            let name = sealed.names.name(&hash.0)?;
            let mut text = String::new();
            text.try_reserve_exact(name.len().saturating_mul(2).saturating_add(extension.len()).saturating_add(1))
                .map_err(|_| BackupError::Capacity)?;
            for byte in name {
                write!(text, "{byte:02x}").map_err(|_| BackupError::Capacity)?;
            }
            text.push('.');
            text.push_str(extension);
            Ok(text)
        }
        /// `bytes` installed durably at `path` in the form: sealed whole first where the backup
        /// seals, the sealed copy charged to `budget` while it is held.
        fn install<M: BackupMedium>(
            &self,
            medium: &mut M,
            path: &Path,
            bytes: &[u8],
            budget: &MemoryBudget,
        ) -> Result<(), BackupError> {
            let Some(sealed) = &self.sealed else {
                return install(medium, path, bytes);
            };
            let bound = focal_seal::file::sealed_bound(bytes.len()).ok_or(BackupError::Capacity)?;
            let _held = budget.reserve(BudgetKind::Recovery, BudgetLane::Completion, bound)?;
            let file = focal_seal::file::seal(&sealed.key, bytes).map_err(file_error)?;
            install(medium, path, &file)
        }
        /// The file at `path` read whole, at most `most` bytes of plaintext: opened where the
        /// backup seals, its sealed bytes charged to `budget` while they are held. A sealed file
        /// that does not open (changed, cut, from another backup) is corrupt.
        fn read<M: BackupMedium>(
            &self,
            medium: &M,
            path: &Path,
            most: usize,
            budget: &MemoryBudget,
        ) -> Result<Vec<u8>, BackupError> {
            let Some(sealed) = &self.sealed else {
                return Ok(medium.read(path, most)?);
            };
            let bound = focal_seal::file::sealed_bound(most).ok_or(BackupError::Capacity)?;
            let _held = budget.reserve(BudgetKind::Recovery, BudgetLane::Completion, bound)?;
            let file = medium.read(path, bound)?;
            let mut plain = focal_seal::file::open(&sealed.key, file, most).map_err(file_error)?;
            Ok(std::mem::take(&mut *plain))
        }
    }
    fn hex_id(id: &KeyId) -> String {
        use std::fmt::Write as _;
        let mut text = String::with_capacity(id.0.len().saturating_mul(2));
        for byte in id.0 {
            let _ = write!(text, "{byte:02x}");
        }
        text
    }
    fn file_error(error: focal_seal::file::FileError) -> BackupError {
        match error {
            focal_seal::file::FileError::Seal(_) => BackupError::Corrupt("a sealed file does not open"),
            focal_seal::file::FileError::Bound(_) => BackupError::Corrupt("a sealed file past its bound"),
        }
    }
    fn content_error(error: BackupError) -> ContentError {
        match error {
            BackupError::Io(error) => ContentError::Io(error),
            BackupError::Content(error) => error,
            BackupError::Capacity | BackupError::Memory(_) => ContentError::Capacity,
            _ => ContentError::Corrupt,
        }
    }

    /// A backup directory as its readers see it (29 §12.8): its medium, its root, its form, and the
    /// budget the sealed copies of what it reads are charged to while they are held.
    pub struct BackupFiles<'a, M> {
        pub medium: &'a M,
        pub root: &'a Path,
        pub form: &'a BackupForm,
        pub budget: &'a MemoryBudget,
    }
    impl<M> Clone for BackupFiles<'_, M> {
        fn clone(&self) -> Self {
            *self
        }
    }
    impl<M> Copy for BackupFiles<'_, M> {}
    impl<M: BackupMedium> BackupFiles<'_, M> {
        fn read(&self, path: &Path, most: usize) -> Result<Vec<u8>, BackupError> {
            self.form.read(self.medium, path, most, self.budget)
        }
        fn seed_path(&self, hash: &ContentHash) -> Result<PathBuf, BackupError> {
            Ok(self.root.join(SEEDS_DIR).join(self.form.name(hash, "seed")?))
        }
        fn cold_path(&self, data: bool, hash: &ContentHash) -> Result<PathBuf, BackupError> {
            Ok(self
                .root
                .join(COLD_DIR)
                .join(cold_part(data))
                .join(self.form.name(hash, "seed")?))
        }
        fn object_path(&self, root: &ContentHash) -> Result<PathBuf, BackupError> {
            Ok(self.root.join(CONTENT_DIR).join(self.form.name(root, "manifest")?))
        }
        fn chunk_path(&self, hash: &ContentHash) -> Result<PathBuf, BackupError> {
            Ok(self.root.join(CONTENT_DIR).join(self.form.name(hash, "chunk")?))
        }
        /// The backup's manifest: a directory without one is not a backup.
        pub fn read_manifest(&self) -> Result<BackupManifest, BackupError> {
            let bytes = match self.read(&self.root.join(MANIFEST_FILE), MAX_MANIFEST_BYTES) {
                Ok(bytes) => bytes,
                Err(BackupError::Io(_)) => return Err(BackupError::Missing("manifest")),
                Err(error) => return Err(error),
            };
            BackupManifest::decode(&bytes)
        }
        /// The session envelope the backup carries.
        pub fn read_checkpoint(&self) -> Result<Vec<u8>, BackupError> {
            self.read(&self.root.join(CHECKPOINT_FILE), MAX_CHECKPOINT_BYTES)
        }
    }

    /// Where the chunks of a seeded Core root are read from: a replica's seed
    /// store while writing, the backup's seed files while verifying.
    pub trait SeedSource {
        fn read_seed(&self, hash: ContentHash, length: usize) -> Result<Vec<u8>, ContentError>;
        /// One chunk of the session's cold data stream (`data`) or its marks (doc 31 M2, A2),
        /// checked against its name; a source that holds no cold streams has none.
        fn read_cold(
            &self,
            _data: bool,
            _hash: ContentHash,
            _length: usize,
        ) -> Result<Vec<u8>, ContentError> {
            Err(ContentError::Invalid)
        }
    }
    /// A replica's seeds and its session's cold streams, where a backup is written from.
    pub struct SessionSeeds {
        pub seeds: SeedReader,
        /// The session's cold directory (`cold/<session>`), when it keeps cold streams.
        pub cold: Option<PathBuf>,
        /// The data directory's keys, which the cold streams' directories' keys are kept under
        /// (29 §12.6).
        pub keys: focal_seal::DataKeys,
    }
    impl SeedSource for SessionSeeds {
        fn read_seed(&self, hash: ContentHash, length: usize) -> Result<Vec<u8>, ContentError> {
            self.seeds.read(hash, length)
        }
        fn read_cold(
            &self,
            data: bool,
            hash: ContentHash,
            length: usize,
        ) -> Result<Vec<u8>, ContentError> {
            let root = self.cold.as_ref().ok_or(ContentError::Invalid)?;
            ColdReader::open(root.join(cold_part(data)), self.keys.clone())?.read(hash, length)
        }
    }
    impl SeedSource for SeedReader {
        fn read_seed(&self, hash: ContentHash, length: usize) -> Result<Vec<u8>, ContentError> {
            self.read(hash, length)
        }
    }
    /// The seed files of a backup directory, read in its form.
    pub struct BackupSeeds<'a, M> {
        files: BackupFiles<'a, M>,
    }
    impl<'a, M: BackupMedium> BackupSeeds<'a, M> {
        pub fn new(files: BackupFiles<'a, M>) -> Self {
            Self { files }
        }
    }
    impl<M: BackupMedium> SeedSource for BackupSeeds<'_, M> {
        fn read_seed(&self, hash: ContentHash, length: usize) -> Result<Vec<u8>, ContentError> {
            let bytes = self
                .files
                .seed_path(&hash)
                .and_then(|path| self.files.read(&path, length))
                .map_err(content_error)?;
            if bytes.len() != length || ContentHash(*blake3::hash(&bytes).as_bytes()) != hash {
                return Err(ContentError::Corrupt);
            }
            Ok(bytes)
        }
        fn read_cold(
            &self,
            data: bool,
            hash: ContentHash,
            length: usize,
        ) -> Result<Vec<u8>, ContentError> {
            let bytes = self
                .files
                .cold_path(data, &hash)
                .and_then(|path| self.files.read(&path, length))
                .map_err(content_error)?;
            if ContentHash(*blake3::hash(&bytes).as_bytes()) != hash {
                return Err(ContentError::Corrupt);
            }
            Ok(bytes)
        }
    }
    /// Every chunk a cold stream's `state` names, in order, with its length: each manifest chunk,
    /// then the chunks it names, then the names after the last one.
    fn cold_chunks(
        state: &ColdState,
        geometry: ColdGeometry,
        read: &dyn Fn(ContentHash, usize) -> Result<Vec<u8>, ContentError>,
    ) -> Result<Vec<BackupChunk>, BackupError> {
        let chunk = u32::try_from(geometry.chunk_bytes).map_err(|_| BackupError::Capacity)?;
        let manifest_bytes = geometry
            .manifest_names
            .checked_mul(32)
            .ok_or(BackupError::Capacity)?;
        let manifest = u32::try_from(manifest_bytes).map_err(|_| BackupError::Capacity)?;
        let mut chunks = Vec::new();
        for hash in &state.manifests {
            chunks.try_reserve(1).map_err(|_| BackupError::Capacity)?;
            chunks.push(BackupChunk {
                hash: *hash,
                length: manifest,
            });
            let listed = read(*hash, manifest_bytes)?;
            if listed.len() != manifest_bytes {
                return Err(BackupError::Corrupt("cold manifest chunk"));
            }
            chunks
                .try_reserve(geometry.manifest_names)
                .map_err(|_| BackupError::Capacity)?;
            let (names, rest) = listed.as_chunks::<32>();
            if !rest.is_empty() {
                return Err(BackupError::Corrupt("cold manifest chunk"));
            }
            chunks.extend(names.iter().map(|name| BackupChunk {
                hash: ContentHash(*name),
                length: chunk,
            }));
        }
        chunks
            .try_reserve(state.names.len())
            .map_err(|_| BackupError::Capacity)?;
        chunks.extend(state.names.iter().map(|hash| BackupChunk {
            hash: *hash,
            length: chunk,
        }));
        Ok(chunks)
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
    /// The content files of a backup directory, read in its form.
    pub struct BackupContent<'a, M> {
        files: BackupFiles<'a, M>,
    }
    impl<'a, M: BackupMedium> BackupContent<'a, M> {
        pub fn new(files: BackupFiles<'a, M>) -> Self {
            Self { files }
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
        fn manifest_chunks(&self, reference: &ContentRef) -> Result<usize, ContentError> {
            Ok(self.describe(reference.domain, reference.root)?.chunks())
        }
    }
    impl<M: BackupMedium> ObjectSource for BackupContent<'_, M> {
        fn describe(
            &self,
            domain: ContentDomainId,
            root: ContentHash,
        ) -> Result<TransferManifest, ContentError> {
            let bytes = self
                .files
                .object_path(&root)
                .and_then(|path| self.files.read(&path, MAX_TRANSFER_MANIFEST_BYTES))
                .map_err(content_error)?;
            describe_encoded(domain, root, bytes)
        }
        fn chunk(&self, transfer: &TransferManifest, index: usize) -> Result<Vec<u8>, ContentError> {
            let (hash, length) = transfer.chunk(index)?;
            let bytes = self
                .files
                .chunk_path(&hash)
                .and_then(|path| self.files.read(&path, length as usize))
                .map_err(content_error)?;
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
        /// The session's cold streams the envelope names (doc 31 M2, A2).
        pub cold: Option<ColdStreams>,
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
        /// The session's cold streams, when the envelope names them (doc 31 M2, A2).
        pub cold: Option<ColdStreams>,
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
        let cold = match checkpoint.cold() {
            Some(section) => Some(section.streams()?),
            None => None,
        };
        Ok(DecodedImage {
            envelope,
            profile: activation.profile,
            header,
            retention,
            seeds,
            core,
            cold,
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
            seeds: seed_chunks,
            core,
            cold,
            ..
        } = decode_image(checkpoint, seeds, source, limits, budget)?;
        let domain = limits.content_domain;
        let mut objects = BTreeSet::new();
        let mut bundles = Vec::new();
        let mut colds = Vec::new();
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
                    // A seal's bundle (F12) holds no content of its own.
                    ContentRoot::Seal { root, .. } => {
                        objects.insert(root);
                    }
                    // A cold family's frame lies in the cold streams, which the backup carries
                    // whole; what it names of the content is read from it (doc 31 M2, A2).
                    ContentRoot::Cold {
                        digest, retired_at, ..
                    } => {
                        if !colds.contains(&(retired_at, digest)) {
                            colds.try_reserve_exact(1).map_err(|_| BackupError::Capacity)?;
                            colds.push((retired_at, digest));
                        }
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
            // Charged before it is read, as its length names it.
            let _read = budget
                .reserve(
                    BudgetKind::Query,
                    BudgetLane::Ordinary,
                    usize::try_from(*bytes).map_err(|_| BackupError::Capacity)?,
                )?
                .commit();
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
        if !colds.is_empty() {
            let streams = cold.as_ref().ok_or(BackupError::Missing("cold streams"))?;
            let data = |hash: ContentHash, length: usize| seeds.read_cold(true, hash, length);
            let marks = |hash: ContentHash, length: usize| seeds.read_cold(false, hash, length);
            for (retired_at, digest) in &colds {
                let frame = crate::native_session::cold::frame_in(
                    streams,
                    &data,
                    &marks,
                    *retired_at,
                    *digest,
                    budget,
                )?
                .ok_or(BackupError::Missing("cold frame"))?;
                let archive = StructuralArchive::inspect(frame.bytes(), limits.inspection)?;
                let inner = archive.header();
                if inner.ledger != header.ledger {
                    return Err(BackupError::Corrupt("cold frame ledger"));
                }
                for root in inner.content.iter().chain(&inner.inline) {
                    objects.insert(*root);
                }
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
            seeds: seed_chunks,
            objects: sorted,
            retention,
            bundles: u64::try_from(bundles.len().saturating_add(colds.len()))
                .map_err(|_| BackupError::Capacity)?,
            cold,
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
            crate::native_checkpoint::Sections {
                retention: decoded.retention,
                movement: None,
                // The cold streams go with the rows: the restore imports their chunks.
                cold: decoded.cold.as_ref(),
            },
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

    /// A backup's content imported a slice at a time (the audit's F51): what
    /// [`import_content`] does at once — each object's manifest read from the
    /// backup and prepared, each of its chunks read and installed as a
    /// custody transfer installs it, then its completion (every chunk read
    /// back and the stream verified) before its manifest is published — one
    /// chunk a call ([`ContentImport::advance`]), so a node's one content
    /// owner serves other work between chunks. Held by its one caller with
    /// what it charges; dropped, nothing of an unfinished object is
    /// published.
    pub struct ContentImport {
        object: usize,
        current: Option<ImportingObject>,
        imported: u64,
    }
    struct ImportingObject {
        transfer: TransferManifest,
        next: usize,
        completion: Option<focal_evidence::ImportCompletion>,
        _descriptor: focal_memory::Allocation,
    }
    impl Default for ContentImport {
        fn default() -> Self {
            Self::new()
        }
    }
    impl ContentImport {
        pub fn new() -> Self {
            Self {
                object: 0,
                current: None,
                imported: 0,
            }
        }
        /// The objects imported so far.
        pub fn imported(&self) -> u64 {
            self.imported
        }
        /// One slice: the next object's manifest prepared, one of its chunks
        /// read from the backup into the store, or one chunk of its
        /// completion read back into `scratch`. The objects imported, once
        /// every object the backup lists is.
        pub fn advance<M: BackupMedium>(
            &mut self,
            files: BackupFiles<'_, M>,
            manifest: &BackupManifest,
            store: &mut focal_evidence::ContentStore,
            scratch: &mut [u8],
        ) -> Result<Option<u64>, BackupError> {
            let budget = files.budget;
            if self.current.is_none() {
                let Some(object) = manifest.content.get(self.object) else {
                    return Ok(Some(self.imported));
                };
                let reference = ContentRef {
                    domain: manifest.domain,
                    root: object.root,
                    length: object.length,
                    class: object.class,
                };
                let encoded = files.read(
                    &files.object_path(&object.root)?,
                    MAX_TRANSFER_MANIFEST_BYTES,
                )?;
                let descriptor = budget
                    .reserve(
                        BudgetKind::Recovery,
                        BudgetLane::Completion,
                        encoded
                            .len()
                            .checked_mul(4)
                            .and_then(|n| n.checked_add(4096))
                            .ok_or(BackupError::Capacity)?,
                    )?
                    .commit();
                let transfer = store.prepare_import(reference, encoded)?;
                self.current = Some(ImportingObject {
                    transfer,
                    next: 0,
                    completion: None,
                    _descriptor: descriptor,
                });
                return Ok(None);
            }
            let current = self.current.as_mut().ok_or(BackupError::Capacity)?;
            if current.next < current.transfer.chunks() {
                let (_, length) = current.transfer.chunk(current.next)?;
                let _chunk = budget.reserve(
                    BudgetKind::Recovery,
                    BudgetLane::Completion,
                    (length as usize)
                        .checked_mul(2)
                        .ok_or(BackupError::Capacity)?,
                )?;
                let bytes = BackupContent::new(files).chunk(&current.transfer, current.next)?;
                store.import_chunk(&current.transfer, current.next, &bytes)?;
                current.next = current.next.checked_add(1).ok_or(BackupError::Capacity)?;
                return Ok(None);
            }
            if current.completion.is_none() {
                current.completion = Some(store.begin_completion(&current.transfer)?);
            }
            let completion = current.completion.as_mut().ok_or(BackupError::Capacity)?;
            let Some(installed) = store.advance_completion(&current.transfer, completion, scratch)?
            else {
                return Ok(None);
            };
            if installed != *current.transfer.reference() {
                return Err(BackupError::Corrupt("imported object"));
            }
            self.current = None;
            self.object = self.object.checked_add(1).ok_or(BackupError::Capacity)?;
            self.imported = self.imported.saturating_add(1);
            Ok(None)
        }
    }

    /// Import every object a backup lists into `store`, chunk by chunk,
    /// exactly as a custody transfer installs them (each chunk verified,
    /// the manifest published only once every chunk is local). Objects the
    /// store already holds are verified in place. At once: a
    /// [`ContentImport`] to its end, for a caller that holds the store alone.
    pub fn import_content<M: BackupMedium>(
        files: BackupFiles<'_, M>,
        manifest: &BackupManifest,
        store: &mut focal_evidence::ContentStore,
    ) -> Result<u64, BackupError> {
        let _scratch = files.budget.reserve(
            BudgetKind::Recovery,
            BudgetLane::Completion,
            store.max_chunk_bytes(),
        )?;
        let mut scratch = Vec::new();
        scratch
            .try_reserve_exact(store.max_chunk_bytes())
            .map_err(|_| BackupError::Capacity)?;
        scratch.resize(store.max_chunk_bytes(), 0);
        // A slice for each object's manifest, each of its chunks twice (read
        // in, read back) — a chunk holds a byte at least, so an object has as
        // many chunks as bytes at most — and its publication; one to end.
        let slices = manifest
            .content
            .iter()
            .try_fold(1u64, |sum, object| {
                object
                    .length
                    .checked_mul(2)
                    .and_then(|chunks| chunks.checked_add(2))
                    .and_then(|slices| sum.checked_add(slices))
            })
            .ok_or(BackupError::Capacity)?;
        let mut import = ContentImport::new();
        let mut taken = 0u64;
        while taken < slices {
            if let Some(imported) = import.advance(files, manifest, store, &mut scratch)? {
                return Ok(imported);
            }
            taken = taken.saturating_add(1);
        }
        Err(BackupError::Capacity)
    }
    /// Install every cold chunk a backup lists into the restored session's cold directory
    /// `target` (`cold/<session>`), each verified against its name (doc 31 M2, A2), in the form
    /// `form` under the data directory's `keys` (29 §12.6).
    pub fn import_cold<M: BackupMedium>(
        files: BackupFiles<'_, M>,
        manifest: &BackupManifest,
        target: &Path,
        disk: &focal_memory::DiskBudget,
        keys: &focal_seal::DataKeys,
        form: focal_evidence::ContentForm,
    ) -> Result<u64, BackupError> {
        let seeds = BackupSeeds::new(files);
        let mut installed = 0u64;
        for (data, chunks) in [(true, &manifest.cold_data), (false, &manifest.cold_marks)] {
            for chunk in chunks {
                let bytes = seeds.read_cold(data, chunk.hash, chunk.length as usize)?;
                focal_evidence::install_cold_chunk(
                    &target.join(cold_part(data)),
                    disk,
                    keys,
                    form,
                    chunk.hash,
                    &bytes,
                )?;
                installed = installed.saturating_add(1);
            }
        }
        Ok(installed)
    }
    /// Install every seed chunk a backup lists into `sink`.
    pub fn import_seeds<M: BackupMedium>(
        files: BackupFiles<'_, M>,
        manifest: &BackupManifest,
        sink: &mut focal_evidence::SeedStore,
    ) -> Result<u64, BackupError> {
        let seeds = BackupSeeds::new(files);
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
    /// Write a backup of `image` under `root` in `form`: the envelope, the
    /// seed chunks it names, the exact tree of every object its prefix names,
    /// and the manifest last; sealed, its `ENCRYPTION-INFO` before any of them
    /// (29 §12.8). Refuses a directory that already holds a manifest, or part
    /// of a backup sealed under another key.
    #[allow(
        clippy::too_many_arguments,
        reason = "one bounded pass over borrowed sources; nothing is retained"
    )]
    pub fn write<M: BackupMedium, S: ObjectSource>(
        medium: &mut M,
        root: &Path,
        form: &BackupForm,
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
        if medium.exists(&root.join(MANIFEST_FILE))? {
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
        // Sealed, the key's record first: no sealed file stands without the record that opens it.
        // A record already there is this backup's, from a write a crash cut, when it is the same.
        if let Some(sealed) = &form.sealed {
            let path = root.join(ENCRYPTION_INFO_FILE);
            if medium.exists(&path)? {
                if medium.read(&path, KEY_INFO)? != sealed.info {
                    return Err(BackupError::Exists);
                }
            } else {
                install(medium, &path, &sealed.info)?;
            }
        }
        medium.create_dir(&root.join(SEEDS_DIR))?;
        medium.create_dir(&root.join(CONTENT_DIR))?;
        form.install(medium, &root.join(CHECKPOINT_FILE), &image.checkpoint, budget)?;
        let mut files = 1u64;
        let mut bytes = image.checkpoint.len() as u64;
        // The cold streams' chunks, every one the envelope names (doc 31 M2, A2).
        let mut cold_lists = [Vec::new(), Vec::new()];
        if let Some(streams) = inventory.cold.as_ref() {
            medium.create_dir(&root.join(COLD_DIR))?;
            for (index, (data, state)) in [(true, &streams.data), (false, &streams.marks)]
                .into_iter()
                .enumerate()
            {
                medium.create_dir(&root.join(COLD_DIR).join(cold_part(data)))?;
                let read = |hash: ContentHash, length: usize| seeds.read_cold(data, hash, length);
                let chunks = cold_chunks(state, streams.geometry, &read)?;
                for chunk in &chunks {
                    let length = usize::try_from(chunk.length).map_err(|_| BackupError::Capacity)?;
                    let _scratch =
                        budget.reserve(BudgetKind::Recovery, BudgetLane::Completion, length)?;
                    let read = seeds.read_cold(data, chunk.hash, length)?;
                    form.install(
                        medium,
                        &root
                            .join(COLD_DIR)
                            .join(cold_part(data))
                            .join(form.name(&chunk.hash, "seed")?),
                        &read,
                        budget,
                    )?;
                    files = files.saturating_add(1);
                    bytes = bytes.saturating_add(u64::from(chunk.length));
                }
                if let Some(list) = cold_lists.get_mut(index) {
                    *list = chunks;
                }
            }
        }
        let [cold_data, cold_marks] = cold_lists;
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
            form.install(
                medium,
                &root.join(SEEDS_DIR).join(form.name(&chunk.hash, "seed")?),
                &read,
                budget,
            )?;
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
            form.install(
                medium,
                &root.join(CONTENT_DIR).join(form.name(object, "manifest")?),
                transfer.encoded(),
                budget,
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
                    form.install(
                        medium,
                        &root.join(CONTENT_DIR).join(form.name(&hash, "chunk")?),
                        &read,
                        budget,
                    )?;
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
            cold_data,
            cold_marks,
        };
        manifest.validate()?;
        form.install(medium, &root.join(MANIFEST_FILE), &manifest.encode()?, budget)?;
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
    /// Verify a backup file by file in its form (29 §12.8), then rebuild its
    /// envelope against its own content files and compare what it names with
    /// what the manifest lists. Nothing is written.
    pub fn verify<M: BackupMedium>(
        files: BackupFiles<'_, M>,
        native_decoder: [u8; 32],
    ) -> Result<VerifyReport, BackupError> {
        let budget = files.budget;
        let manifest = files.read_manifest()?;
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
        let checkpoint = match files.read_checkpoint() {
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
            match files
                .seed_path(&chunk.hash)
                .and_then(|path| files.read(&path, chunk.length as usize))
            {
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
        // The cold streams' chunks (doc 31 M2, A2), each against its name.
        let backup_seeds = BackupSeeds::new(files);
        for (data, chunks) in [(true, &manifest.cold_data), (false, &manifest.cold_marks)] {
            for chunk in chunks {
                match backup_seeds.read_cold(data, chunk.hash, chunk.length as usize) {
                    Ok(bytes) if bytes.len() == chunk.length as usize => {
                        seeds_verified = seeds_verified.saturating_add(1);
                    }
                    Ok(_) => problems.push(format!("cold chunk {}: length differs", chunk.hash)),
                    Err(error) => problems.push(format!("cold chunk {}: {error}", chunk.hash)),
                }
            }
        }
        let domain = manifest.domain;
        let content = BackupContent::new(files);
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
            let seeds = BackupSeeds::new(files);
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
                        // The cold chunks the envelope names are the ones the manifest lists.
                        let cold_match = match inventory.cold.as_ref() {
                            None => manifest.cold_data.is_empty() && manifest.cold_marks.is_empty(),
                            Some(streams) => [
                                (true, &streams.data, &manifest.cold_data),
                                (false, &streams.marks, &manifest.cold_marks),
                            ]
                            .into_iter()
                            .all(|(data, state, listed)| {
                                let read = |hash: ContentHash, length: usize| {
                                    seeds.read_cold(data, hash, length)
                                };
                                cold_chunks(state, streams.geometry, &read)
                                    .is_ok_and(|named| named == *listed)
                            }),
                        };
                        if !cold_match {
                            problems.push("inventory: cold chunks differ from the manifest".to_owned());
                        }
                        inventory.objects == listed && seeds_match && coordinates && cold_match
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
