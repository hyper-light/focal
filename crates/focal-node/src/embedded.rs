//! Local ownership and durable bootstrap. The same Raft session format is used by
//! the network host. This owner exposes no unauthenticated remote API.
use crate::{
    config::Settings,
    placement::{self, NodeFacts},
};
use focal_consensus::NodeConfig;
use focal_consensus::storage_open::{OpenError, OpenedStorage};
use focal_evidence::{ContentStore, StoreLimits};
use focal_ledger::{LedgerError, Session};
use focal_log::WalIdentity;
use focal_model::*;
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::Path,
};

const IDENTITY_MAGIC: &[u8] = b"FOCALND1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeIdentity {
    pub schema: u16,
    pub cluster: [u8; 16],
    pub node: u64,
    pub ledger: LedgerId,
    pub issuer: ParticipantId,
    pub worker: ParticipantId,
    pub evaluator: ParticipantId,
    pub root: RootCommandId,
}
impl NodeIdentity {
    pub(crate) fn validate(&self) -> Result<(), NodeError> {
        if self.schema != 1
            || self.node == 0
            || self.cluster == [0; 16]
            || self.ledger.tenant.is_zero()
            || self.ledger.session.is_zero()
            || self.issuer.is_zero()
            || self.worker.is_zero()
            || self.evaluator.is_zero()
            || self.root.is_zero()
        {
            return Err(NodeError::Identity);
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("configuration: {0}")]
    Config(#[from] crate::config::ConfigError),
    #[error("ledger: {0}")]
    Ledger(#[from] LedgerError),
    #[error("consensus: {0}")]
    Consensus(#[from] focal_consensus::ConsensusError),
    /// Boxed: what opening the storage refuses carries the log's and the conversion's errors,
    /// which every other `NodeError` need not be the size of.
    #[error("the node's storage: {0}")]
    Storage(Box<focal_consensus::storage_open::OpenError>),
    #[error("WAL: {0}")]
    Wal(#[from] focal_log::LogError),
    #[error("content: {0}")]
    Content(#[from] focal_evidence::ContentError),
    #[error("encoding: {0}")]
    Encoding(#[from] postcard::Error),
    #[error("canonical content: {0}")]
    Canonical(#[from] focal_model::CanonicalError),
    /// The data directory is not this user's, or another user may write to
    /// it and so replace the node's identity or a journal's entry: refused
    /// at start, before anything is kept there. One others may read is the
    /// node's: what it keeps private it opens private.
    #[error(
        "data directory {} must be this user's and writable by no other user (`focal prepare volume --owner UID:GID`)",
        .0.display()
    )]
    NotPrivate(std::path::PathBuf),
    #[error("data directory already has a live owner")]
    Locked,
    #[error("node identity or stored policy is corrupt or incompatible")]
    Identity,
    /// A record of the data directory that is there and does not read whole.
    #[error("corrupt node record: {0}")]
    Corrupt(&'static str),
    #[error("deployment cannot satisfy its configured guarantee: {0}")]
    Placement(String),
    #[error("this embedded owner requires local operation; use the network host for peers")]
    NetworkRequired,
    #[error("operating-system randomness failed: {0}")]
    Entropy(String),
    #[error("domain refused the operation: {0}")]
    Domain(String),
    /// The data directory's keys did not open or could not be made (29 §2).
    #[error("the data directory's keys: {0}")]
    Keys(#[from] focal_seal::SealSetupError),
}
impl From<focal_consensus::storage_open::OpenError> for NodeError {
    fn from(error: focal_consensus::storage_open::OpenError) -> Self {
        Self::Storage(Box::new(error))
    }
}

/// The fields drop in the order a node's storage is let go (`close`): the session that writes
/// through the log, then the content store, then the storage (its handle, the shell's log, its
/// cache), and the data directory's lock last.
pub struct EmbeddedNode {
    pub identity: NodeIdentity,
    pub session: Session,
    pub content: ContentStore,
    /// The node's storage as a networked node's start opens it (`storage_start`): hyper-log's log
    /// under the shell, sealed under the data directory's root key (29 §2), its WAL converted
    /// where an earlier binary left one. An embedded node wrote focal-log's WAL in the clear.
    pub(crate) storage: OpenedStorage,
    directory: crate::node_directory::NodeDirectory,
    /// The data directory's keys (29 §2), made at its first start: what its sealed records are
    /// sealed under.
    keys: focal_seal::DataKeys,
}

impl EmbeddedNode {
    pub fn open(settings: &Settings) -> Result<Self, NodeError> {
        settings.validate()?;
        if !settings.node.seeds.is_empty()
            || settings.node.listen.is_some()
            || settings.node.advertise.is_some()
        {
            return Err(NodeError::NetworkRequired);
        }
        let directory = crate::node_directory::NodeDirectory::open(settings)?;
        let root = directory.root();
        if crate::network_state::network_requested(root, settings) {
            return Err(NodeError::NetworkRequired);
        }
        let keys = settings.data_keys(root)?;
        let identity = directory.identity().clone();
        // An initialized store's committed policy is checked before anything
        // else: a differing file is refused by its field, never re-solved.
        let committed = check_policy(root, settings)?;
        let facts = [NodeFacts {
            id: identity.node,
            topology: settings.topology.clone(),
            verified: true,
            eligible: true,
        }];
        placement::plan(&facts, &settings.durability, &settings.placement)
            .map_err(|e| NodeError::Placement(e.to_string()))?;
        if committed.is_none() {
            install_policy(root, settings)?;
        }
        // An embedded node is its own cluster: the level its binary announces is its fence, the
        // storage level among it, so its groups live on the shell from its first start.
        let level = crate::upgrade::announced_level();
        let budget = crate::memory_envelope::node_budget(settings).map_err(LedgerError::from)?;
        let storage = crate::storage_start::open(
            root,
            WalIdentity {
                cluster: identity.cluster,
                node: identity.node,
                stream: 0,
            },
            &budget,
            &keys.key_file,
            level,
        )?;
        let config = NodeConfig::single(identity.node, identity.cluster, identity.ledger.session.0);
        let consensus = storage
            .storage
            .open_member(config, &budget, focal_ledger::entry_needs)?;
        // The content writer exists before the session so a native ledger can
        // read its evidence during recovery.
        let content = ContentStore::open_with_disk(
            root.join("content"),
            StoreLimits {
                max_content_bytes: 64 * 1024 * 1024,
                max_staging_bytes: 128 * 1024 * 1024,
                max_uploads: 16,
                chunk_bytes: 1024 * 1024,
                max_manifest_bytes: 1024 * 1024,
            },
            storage.storage.disk_budget(),
            &keys,
            crate::storage_level::content_form_at(level),
        )?;
        let limits = crate::memory_envelope::found_limits(root, settings, &consensus)?;
        let rule = if level >= crate::upgrade::SEAL_LEVEL {
            focal_ledger::SealRule::Anchored
        } else {
            focal_ledger::SealRule::Derived
        };
        let retire = crate::storage_level::retire_rule_at(level);
        let hosting = crate::network_service::native_hosting(
            root,
            &identity,
            content.reader()?,
            storage.storage.disk_budget(),
            &limits,
            crate::storage_level::Rules {
                seal: rule,
                retire,
                share: crate::storage_level::native_share_at(level),
            },
        )?;
        let mut session = Session::from_node_hosted(identity.ledger, consensus, limits, hosting)?;
        session.campaign()?;
        // One voter can establish the committed current-term read barrier locally.
        for _ in 0..4 {
            let _ = session.poll()?;
        }
        Ok(Self {
            identity,
            session,
            content,
            storage,
            directory,
            keys,
        })
    }

    /// The node let go in the order its storage needs, with what its log's close found: the
    /// session that writes through the log first, then the content store, the storage's handle,
    /// the shell's log (its last answers given, its threads ended), its cache, and the data
    /// directory's lock last, as a networked node's shutdown does (`network_service`). Dropped
    /// instead, the node lets go in the same order and a failed close goes unreported.
    pub fn close(self) -> Result<(), NodeError> {
        let Self {
            session,
            content,
            storage,
            directory,
            ..
        } = self;
        drop(session);
        drop(content);
        let OpenedStorage {
            storage,
            log,
            cache,
            // The store keys beside the log, wiped as they drop (29 §8).
            keys: _,
        } = storage;
        drop(storage);
        let closed = log.map(focal_consensus::ShellLog::close).transpose();
        drop(cache);
        drop(directory);
        closed
            .map(drop)
            .map_err(|error| OpenError::Log(error).into())
    }

    pub fn root(&self) -> &Path {
        self.directory.root()
    }
    /// The data directory's keys (29 §2).
    pub fn keys(&self) -> &focal_seal::DataKeys {
        &self.keys
    }
    pub fn checkpoint(&mut self) -> Result<(), NodeError> {
        self.session.checkpoint()?;
        Ok(())
    }
}

fn random_id() -> Result<[u8; 16], NodeError> {
    let mut id = [0; 16];
    loop {
        getrandom::fill(&mut id).map_err(|e| NodeError::Entropy(e.to_string()))?;
        if id != [0; 16] {
            return Ok(id);
        }
    }
}
pub(crate) fn new_identity() -> Result<NodeIdentity, NodeError> {
    let mut node = 0;
    while node == 0 {
        node = u64::from_be_bytes(
            random_id()?
                .get(..8)
                .ok_or(NodeError::Identity)?
                .try_into()
                .map_err(|_| NodeError::Identity)?,
        );
    }
    Ok(NodeIdentity {
        schema: 1,
        cluster: random_id()?,
        node,
        ledger: LedgerId {
            tenant: TenantId(random_id()?),
            session: SessionId(random_id()?),
        },
        issuer: ParticipantId(random_id()?),
        worker: ParticipantId(random_id()?),
        evaluator: ParticipantId(random_id()?),
        root: RootCommandId(random_id()?),
    })
}
pub fn decode_identity(path: &Path) -> Result<NodeIdentity, NodeError> {
    let bytes = read_bounded(path, 4096)?;
    let payload = bytes.get(40..).ok_or(NodeError::Identity)?;
    if bytes.get(..8) != Some(IDENTITY_MAGIC)
        || Some(blake3::hash(payload).as_bytes().as_slice()) != bytes.get(8..40)
    {
        return Err(NodeError::Identity);
    }
    let (id, trailing): (NodeIdentity, _) = postcard::take_from_bytes(payload)?;
    if !trailing.is_empty() {
        return Err(NodeError::Identity);
    }
    id.validate()?;
    Ok(id)
}
pub(crate) fn read_bounded(path: &Path, max: usize) -> Result<Vec<u8>, NodeError> {
    let file = File::open(path)?;
    if file.metadata()?.len() > max as u64 {
        return Err(NodeError::Identity);
    }
    let mut bytes = Vec::new();
    file.take(
        u64::try_from(max)
            .ok()
            .and_then(|n| n.checked_add(1))
            .ok_or(NodeError::Identity)?,
    )
    .read_to_end(&mut bytes)?;
    if bytes.len() > max {
        return Err(NodeError::Identity);
    }
    Ok(bytes)
}
/// Pin the configured guarantee before creating a durable store, or check
/// it against the store's committed policy (doc 08 §2): a field that
/// differs is refused by name and directs the operator to plan and apply;
/// a missing policy after initialization is data loss, not permission to
/// select a new guarantee. Existing policy bytes keep their original
/// serialized format.
pub(crate) fn install_policy(
    root: &Path,
    settings: &Settings,
) -> Result<crate::config::CommittedPolicy, NodeError> {
    Ok(crate::config::policy::install_or_check(
        root,
        &settings.policy_intent(),
        atomic_file,
    )?)
}
/// The committed policy of an initialized store; `None` for a fresh
/// directory that has no policy yet. A committed policy is authoritative:
/// it is what the fleet already carries, and `apply deployment` may have
/// committed stronger durability than the file's first-start seed, so a
/// differing `settings` value is not a refusal (that would keep the node
/// from ever restarting after a sanctioned apply). Config resolution
/// already yields the committed value for a start; this only reads it.
pub(crate) fn check_policy(
    root: &Path,
    _settings: &Settings,
) -> Result<Option<crate::config::CommittedPolicy>, NodeError> {
    Ok(crate::config::policy::read_committed(root)?)
}
pub(crate) fn durable_dir(path: &Path) -> Result<(), std::io::Error> {
    focal_platform::fs::create_dir_all_private(path)
}
pub(crate) fn atomic_file(path: &Path, bytes: &[u8]) -> Result<(), std::io::Error> {
    let temporary = path.with_extension("install");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    // Close before rename: Windows refuses to rename a file with an open handle.
    drop(file);
    focal_platform::fs::atomic_replace(&temporary, path)?;
    focal_platform::sync_dir(path.parent().unwrap_or_else(|| Path::new(".")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    /// A claim written through `node`'s session whose description is `marker`.
    fn write_marked(node: &mut EmbeddedNode, marker: &str) -> ClaimId {
        // The issuer's request generation first, as every writer negotiates its own (F12).
        let negotiate = crate::demo::request(
            &node.identity,
            "sealed-log-epoch",
            node.identity.issuer,
            Command::NegotiateEpoch {
                epoch: RequestEpoch(1),
            },
            vec![],
        );
        let negotiated = node.session.submit_local(&negotiate).unwrap();
        assert!(
            matches!(negotiated, focal_ledger::Submission::Committed(_)),
            "{negotiated:?}"
        );
        let id = ClaimId::from_u128(0x5ea1);
        let mut claim = crate::demo::claim(&node.identity, id).unwrap();
        claim.content.description = marker.into();
        let input = crate::demo::request(
            &node.identity,
            "sealed-log",
            node.identity.issuer,
            Command::GenerateClaim { claim },
            vec![],
        );
        let result = node.session.submit_local(&input).unwrap();
        assert!(
            matches!(result, focal_ledger::Submission::Committed(_)),
            "{result:?}"
        );
        id
    }
    /// A marker no file holds by chance: a fixed text and random bytes.
    fn marker() -> String {
        let mut bytes = [0u8; 16];
        getrandom::fill(&mut bytes).unwrap();
        let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
        format!("embedded sealed log marker {hex}")
    }
    /// Every file under `dir` whose bytes hold `needle`, walked with a stack of its own.
    fn holding(dir: &Path, needle: &[u8]) -> Vec<std::path::PathBuf> {
        let mut found = Vec::new();
        let mut stack = vec![dir.to_path_buf()];
        while let Some(at) = stack.pop() {
            for entry in fs::read_dir(&at).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    stack.push(path);
                } else if fs::read(&path)
                    .unwrap()
                    .windows(needle.len())
                    .any(|window| window == needle)
                {
                    found.push(path);
                }
            }
        }
        found
    }

    /// An embedded node's session lives on the shell's log, sealed under its data directory's
    /// root key (29 §12.3), as a networked node's does. Do: write a claim whose description is a
    /// marker and let the node go, its log closed. Expect: no WAL; no file of the log and its
    /// group files holds the marker; the claim reads back after a reopen.
    #[test]
    fn an_embedded_nodes_session_lives_on_the_sealed_log() {
        let dir = tempfile::tempdir().unwrap();
        let mut settings = Settings::default();
        settings.node.data_dir = Some(dir.path().to_owned());
        let marker = marker();
        let mut node = EmbeddedNode::open(&settings).unwrap();
        let id = write_marked(&mut node, &marker);
        node.close().unwrap();
        assert!(!dir.path().join(focal_consensus::convert::WAL_DIR).exists());
        assert_eq!(
            holding(
                &dir.path().join(focal_consensus::convert::RAFT_DIR),
                marker.as_bytes()
            ),
            Vec::<std::path::PathBuf>::new()
        );
        let node = EmbeddedNode::open(&settings).unwrap();
        assert!(
            node.session
                .read_at_least(focal_ledger::SessionSeq(0))
                .unwrap()
                .claims
                .contains_key(&id)
        );
        node.close().unwrap();
    }

    /// A data directory an earlier binary left on focal-log's WAL is converted at the node's next
    /// start, as a networked node's is at the storage level: its session's records are kept, now
    /// in the sealed log. Do: found the node below the storage level and write a marked claim (the
    /// WAL holds it in the clear); reopen at the compiled level. Expect: the claim reads back; no
    /// file of the log holds the marker; the old segments wait in `wal-converted/`, still in the
    /// clear, for the operator.
    #[test]
    fn an_embedded_node_left_on_the_wal_converts_and_keeps_its_session() {
        let dir = tempfile::tempdir().unwrap();
        let mut settings = Settings::default();
        settings.node.data_dir = Some(dir.path().to_owned());
        let marker = marker();
        let id = {
            let _level = crate::upgrade::announce_for_test(crate::upgrade::BELOW_STORAGE_LEVEL);
            let mut node = EmbeddedNode::open(&settings).unwrap();
            let id = write_marked(&mut node, &marker);
            node.close().unwrap();
            id
        };
        assert!(
            !holding(
                &dir.path().join(focal_consensus::convert::WAL_DIR),
                marker.as_bytes()
            )
            .is_empty(),
            "the WAL held the record in the clear"
        );
        let node = EmbeddedNode::open(&settings).unwrap();
        assert!(
            node.session
                .read_at_least(focal_ledger::SessionSeq(0))
                .unwrap()
                .claims
                .contains_key(&id)
        );
        node.close().unwrap();
        assert_eq!(
            holding(
                &dir.path().join(focal_consensus::convert::RAFT_DIR),
                marker.as_bytes()
            ),
            Vec::<std::path::PathBuf>::new()
        );
        assert!(
            !holding(
                &dir.path().join(focal_consensus::convert::CONVERTED_DIR),
                marker.as_bytes()
            )
            .is_empty()
        );
    }
    #[test]
    fn identity_and_owner_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        let mut settings = Settings::default();
        settings.node.data_dir = Some(dir.path().to_owned());
        let node = EmbeddedNode::open(&settings).unwrap();
        let id = node.identity.clone();
        assert!(matches!(
            EmbeddedNode::open(&settings),
            Err(NodeError::Locked)
        ));
        drop(node);
        assert_eq!(EmbeddedNode::open(&settings).unwrap().identity, id);
        let mut bytes = fs::read(dir.path().join("IDENTITY")).unwrap();
        bytes[45] ^= 1;
        fs::write(dir.path().join("IDENTITY"), bytes).unwrap();
        assert!(matches!(
            EmbeddedNode::open(&settings),
            Err(NodeError::Identity)
        ));
    }

    #[test]
    fn lost_policy_cannot_be_recreated_beside_an_existing_store() {
        for retain_marker in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut settings = Settings::default();
            settings.node.data_dir = Some(dir.path().to_owned());
            let node = EmbeddedNode::open(&settings).unwrap();
            let identity = node.identity.clone();
            drop(node);
            fs::remove_file(dir.path().join("POLICY")).unwrap();
            if !retain_marker {
                // Also protect deployments created before the marker existed.
                fs::remove_file(dir.path().join("POLICY.initialized")).unwrap();
            }
            assert!(matches!(
                EmbeddedNode::open(&settings),
                Err(NodeError::Config(crate::config::ConfigError::PolicyMissing))
            ));
            assert!(!dir.path().join("POLICY").exists());
            assert_eq!(
                decode_identity(&dir.path().join("IDENTITY")).unwrap(),
                identity
            );
        }
    }

    #[test]
    fn policy_installation_is_exact_and_survives_the_pre_store_crash_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let mut settings = Settings::default();
        settings.node.data_dir = Some(dir.path().to_owned());
        let directory = crate::node_directory::NodeDirectory::open(&settings).unwrap();
        let original =
            postcard::to_stdvec(&(settings.durability.clone(), settings.placement.clone()))
                .unwrap();
        fs::write(dir.path().join("POLICY"), &original).unwrap();
        // The old format is accepted and gains the marker without rewriting it.
        install_policy(directory.root(), &settings).unwrap();
        assert_eq!(fs::read(dir.path().join("POLICY")).unwrap(), original);
        assert!(dir.path().join("POLICY.initialized").is_file());
        let mut changed = settings.clone();
        changed.durability.max_failures = 1;
        assert!(matches!(
            install_policy(directory.root(), &changed),
            Err(NodeError::Config(
                crate::config::ConfigError::CommittedPolicyChange {
                    field: "durability.max_failures"
                }
            ))
        ));
        assert_eq!(fs::read(dir.path().join("POLICY")).unwrap(), original);
        assert!(!dir.path().join("wal").exists());
        fs::remove_file(dir.path().join("POLICY")).unwrap();
        drop(directory);
        assert!(matches!(
            EmbeddedNode::open(&settings),
            Err(NodeError::Config(crate::config::ConfigError::PolicyMissing))
        ));
        assert!(!dir.path().join("POLICY").exists());
        assert!(!dir.path().join("wal").exists());
        // A fresh store pins the revisioned form, which reads back exactly.
        let fresh = tempfile::tempdir().unwrap();
        let mut settings = Settings::default();
        settings.node.data_dir = Some(fresh.path().to_owned());
        let directory = crate::node_directory::NodeDirectory::open(&settings).unwrap();
        let committed = install_policy(directory.root(), &settings).unwrap();
        assert_eq!(committed.revision, crate::config::PolicyRevision(1));
        assert_eq!(
            crate::config::policy::read_committed(directory.root()).unwrap(),
            Some(committed)
        );
    }
}
