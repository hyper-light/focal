//! Local ownership and durable bootstrap. The same Raft session format is used by
//! the network host. This owner exposes no unauthenticated remote API.
use crate::{
    config::Settings,
    placement::{self, NodeFacts},
};
use focal_consensus::{DurableNode, NodeConfig};
use focal_evidence::{ContentStore, StoreLimits};
use focal_ledger::{LedgerError, Session, SessionLimits};
use focal_log::{SharedWal, WalIdentity, WalOptions};
use focal_model::*;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
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
    #[error("WAL: {0}")]
    Wal(#[from] focal_log::LogError),
    #[error("content: {0}")]
    Content(#[from] focal_evidence::ContentError),
    #[error("encoding: {0}")]
    Encoding(#[from] postcard::Error),
    #[error("canonical content: {0}")]
    Canonical(#[from] focal_model::CanonicalError),
    #[error("data directory already has a live owner")]
    Locked,
    #[error("node identity or stored policy is corrupt or incompatible")]
    Identity,
    #[error("deployment cannot satisfy its configured guarantee: {0}")]
    Placement(String),
    #[error("this embedded owner requires local operation; use the network host for peers")]
    NetworkRequired,
    #[error("operating-system randomness failed: {0}")]
    Entropy(String),
    #[error("domain refused the operation: {0}")]
    Domain(String),
}

pub struct EmbeddedNode {
    pub identity: NodeIdentity,
    pub session: Session,
    pub content: ContentStore,
    pub(crate) wal: SharedWal,
    directory: crate::node_directory::NodeDirectory,
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
        let identity = directory.identity().clone();
        let facts = [NodeFacts {
            id: identity.node,
            topology: settings.topology.clone(),
            verified: true,
            eligible: true,
        }];
        placement::plan(&facts, &settings.durability, &settings.placement)
            .map_err(|e| NodeError::Placement(e.to_string()))?;
        install_policy(root, settings)?;
        let wal = SharedWal::open(
            root.join("wal"),
            WalOptions::new(WalIdentity {
                cluster: identity.cluster,
                node: identity.node,
                stream: 0,
            }),
        )?;
        let config = NodeConfig::single(identity.node, identity.cluster, identity.ledger.session.0);
        let consensus = DurableNode::open_on_wal(config, wal.clone())?;
        let mut session = Session::from_node(identity.ledger, consensus, SessionLimits::default())?;
        session.campaign()?;
        // One voter can establish the committed current-term read barrier locally.
        for _ in 0..4 {
            let _ = session.poll()?;
        }
        let content = ContentStore::open(
            root.join("content"),
            StoreLimits {
                max_content_bytes: 64 * 1024 * 1024,
                max_staging_bytes: 128 * 1024 * 1024,
                max_uploads: 16,
                chunk_bytes: 1024 * 1024,
                max_manifest_bytes: 1024 * 1024,
            },
        )?;
        Ok(Self {
            identity,
            session,
            content,
            wal,
            directory,
        })
    }

    pub fn root(&self) -> &Path {
        self.directory.root()
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
/// Pin the configured guarantee before creating a durable store. A missing
/// policy after initialization is data loss, not permission to select a new
/// guarantee. Existing policy bytes keep their original serialized format.
pub(crate) fn install_policy(root: &Path, settings: &Settings) -> Result<(), NodeError> {
    let path = root.join("POLICY");
    let marker = root.join("POLICY.initialized");
    let policy = postcard::to_stdvec(&(&settings.durability, &settings.placement))?;
    if path.exists() {
        if read_bounded(&path, 64 * 1024)? != policy {
            return Err(NodeError::Identity);
        }
    } else {
        if [
            "POLICY.initialized",
            "wal",
            "content",
            "NETWORK",
            "NETWORK.initialized",
        ]
        .iter()
        .any(|name| root.join(name).exists())
        {
            return Err(NodeError::Identity);
        }
        atomic_file(&path, &policy)?;
    }
    if !marker.exists() {
        atomic_file(&marker, b"deployment policy installed")?;
    }
    Ok(())
}
pub(crate) fn durable_dir(path: &Path) -> Result<(), std::io::Error> {
    if path.is_dir() {
        return Ok(());
    }
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        durable_dir(parent)?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        fs::DirBuilder::new().mode(0o700).create(path)?;
    }
    #[cfg(not(unix))]
    fs::create_dir(path)?;
    File::open(path)?.sync_all()?;
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
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
    fs::rename(temporary, path)?;
    File::open(path.parent().unwrap_or_else(|| Path::new(".")))?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;
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
                Err(NodeError::Identity)
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
            Err(NodeError::Identity)
        ));
        assert_eq!(fs::read(dir.path().join("POLICY")).unwrap(), original);
        assert!(!dir.path().join("wal").exists());
        fs::remove_file(dir.path().join("POLICY")).unwrap();
        drop(directory);
        assert!(matches!(
            EmbeddedNode::open(&settings),
            Err(NodeError::Identity)
        ));
        assert!(!dir.path().join("POLICY").exists());
        assert!(!dir.path().join("wal").exists());
    }
}
