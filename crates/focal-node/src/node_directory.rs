//! One durable physical-node identity across local service, network expansion,
//! enrollment installation and restart. The directory lease outlives its stores.
use crate::{
    config::Settings,
    embedded::{NodeError, NodeIdentity, atomic_file, decode_identity, durable_dir, new_identity},
};
use fs2::FileExt;
use std::{
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
};

pub struct NodeDirectory {
    identity: NodeIdentity,
    root: PathBuf,
    _lock: File,
}
/// Owns the physical directory before a joining node has an assigned identity.
pub struct JoinDirectory {
    root: PathBuf,
    existing: Option<NodeIdentity>,
    lock: File,
}
impl JoinDirectory {
    pub fn open(settings: &Settings) -> Result<Self, NodeError> {
        let (root, lock) = acquire(settings)?;
        if root.join("JOIN.initialized").exists() && !root.join("JOIN/journal.bin").is_file() {
            return Err(NodeError::Identity);
        }
        let path = root.join("IDENTITY");
        let existing = if path.exists() {
            if !root.join("JOIN").is_dir() {
                return Err(NodeError::Identity);
            }
            Some(decode_identity(&path)?)
        } else {
            for name in [
                "IDENTITY.initialized",
                "POLICY",
                "POLICY.initialized",
                "wal",
                "content",
                "cluster",
                "NETWORK",
                "NETWORK.initialized",
            ] {
                if root.join(name).exists() {
                    return Err(NodeError::Identity);
                }
            }
            None
        };
        Ok(Self {
            root,
            existing,
            lock,
        })
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn install(self, identity: NodeIdentity) -> Result<NodeDirectory, NodeError> {
        identity.validate()?;
        if let Some(existing) = &self.existing {
            if existing != &identity {
                return Err(NodeError::Identity);
            }
        } else {
            save(&self.root.join("IDENTITY"), &identity)?;
        }
        mark_initialized(&self.root)?;
        Ok(NodeDirectory {
            identity,
            root: self.root,
            _lock: self.lock,
        })
    }
}
impl NodeDirectory {
    /// Existing network and local identities are identical at this boundary.
    /// Network authority is validated by the network owner before opening groups.
    pub fn open(settings: &Settings) -> Result<Self, NodeError> {
        let (root, lock) = acquire(settings)?;
        let path = root.join("IDENTITY");
        let identity = if path.exists() {
            decode_identity(&path)?
        } else {
            // Interrupted enrollment is not a fresh standalone deployment.
            // Recovery must finish its original join, never invent a new cluster.
            for name in [
                "IDENTITY.initialized",
                "POLICY",
                "POLICY.initialized",
                "wal",
                "content",
                "cluster",
                "JOIN",
                "JOIN.initialized",
                "NETWORK",
                "NETWORK.initialized",
            ] {
                if root.join(name).exists() {
                    return Err(NodeError::Identity);
                }
            }
            let identity = new_identity()?;
            save(&path, &identity)?;
            identity
        };
        // A successfully opened identity cannot later be replaced merely
        // because its body was lost before the first WAL group was created.
        mark_initialized(&root)?;
        Ok(Self {
            identity,
            root,
            _lock: lock,
        })
    }
    /// The caller has verified the pinned sponsor's durable enrollment and
    /// bootstrap. Install exactly that identity, idempotently, under the same
    /// exclusive lock used by normal startup. Never replace an existing node.
    pub fn install_joined(settings: &Settings, identity: NodeIdentity) -> Result<Self, NodeError> {
        let (root, lock) = acquire(settings)?;
        let path = root.join("IDENTITY");
        identity.validate()?;
        if path.exists() {
            if decode_identity(&path)? != identity {
                return Err(NodeError::Identity);
            }
        } else {
            for name in [
                "IDENTITY.initialized",
                "POLICY",
                "POLICY.initialized",
                "wal",
                "content",
                "cluster",
                "NETWORK",
                "NETWORK.initialized",
            ] {
                if root.join(name).exists() {
                    return Err(NodeError::Identity);
                }
            }
            save(&path, &identity)?;
        }
        mark_initialized(&root)?;
        Ok(Self {
            identity,
            root,
            _lock: lock,
        })
    }
    pub fn identity(&self) -> &NodeIdentity {
        &self.identity
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
}
fn mark_initialized(root: &Path) -> Result<(), NodeError> {
    let path = root.join("IDENTITY.initialized");
    if !path.exists() {
        atomic_file(&path, b"physical node identity installed")?;
    }
    Ok(())
}
fn acquire(settings: &Settings) -> Result<(PathBuf, File), NodeError> {
    settings.validate()?;
    let root = settings.data_dir()?;
    durable_dir(&root)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(root.join("LOCK"))?;
    lock.try_lock_exclusive().map_err(|error| {
        if error.kind() == std::io::ErrorKind::WouldBlock {
            NodeError::Locked
        } else {
            NodeError::Io(error)
        }
    })?;
    Ok((root, lock))
}
fn save(path: &Path, identity: &NodeIdentity) -> Result<(), NodeError> {
    identity.validate()?;
    let payload = postcard::to_stdvec(identity)?;
    let mut bytes = b"FOCALND1".to_vec();
    bytes.extend_from_slice(blake3::hash(&payload).as_bytes());
    bytes.extend_from_slice(&payload);
    atomic_file(path, &bytes)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn expansion_keeps_identity_and_enrollment_cannot_replace_or_race_it() {
        let root = tempfile::tempdir().unwrap();
        let mut settings = Settings::default();
        settings.node.data_dir = Some(root.path().to_owned());
        let local = NodeDirectory::open(&settings).unwrap();
        let identity = local.identity().clone();
        assert!(matches!(
            NodeDirectory::install_joined(&settings, identity.clone()),
            Err(NodeError::Locked)
        ));
        drop(local);
        settings.node.advertise = Some("127.0.0.1:7443".into());
        let network = NodeDirectory::open(&settings).unwrap();
        assert_eq!(network.identity(), &identity);
        drop(network);
        assert!(NodeDirectory::install_joined(&settings, new_identity().unwrap()).is_err());
        assert_eq!(
            NodeDirectory::open(&settings).unwrap().identity(),
            &identity
        );
    }
    #[test]
    fn interrupted_join_requires_original_identity_and_retries_exact_installation() {
        let root = tempfile::tempdir().unwrap();
        let mut settings = Settings::default();
        settings.node.data_dir = Some(root.path().to_owned());
        durable_dir(&root.path().join("JOIN")).unwrap();
        assert!(matches!(
            NodeDirectory::open(&settings),
            Err(NodeError::Identity)
        ));
        let identity = new_identity().unwrap();
        drop(NodeDirectory::install_joined(&settings, identity.clone()).unwrap());
        assert_eq!(
            NodeDirectory::install_joined(&settings, identity.clone())
                .unwrap()
                .identity(),
            &identity
        );
    }
    #[test]
    fn missing_acknowledged_identity_cannot_initialize_a_replacement_cluster() {
        let root = tempfile::tempdir().unwrap();
        let mut settings = Settings::default();
        settings.node.data_dir = Some(root.path().to_owned());
        let directory = NodeDirectory::open(&settings).unwrap();
        let identity = directory.identity().clone();
        drop(directory);
        std::fs::remove_file(root.path().join("IDENTITY")).unwrap();
        assert!(matches!(
            NodeDirectory::open(&settings),
            Err(NodeError::Identity)
        ));
        assert!(matches!(
            NodeDirectory::install_joined(&settings, identity),
            Err(NodeError::Identity)
        ));
        assert!(!root.path().join("IDENTITY").exists());
        std::fs::remove_file(root.path().join("IDENTITY.initialized")).unwrap();
        std::fs::write(root.path().join("POLICY"), b"old deployment policy").unwrap();
        assert!(matches!(
            NodeDirectory::open(&settings),
            Err(NodeError::Identity)
        ));
    }
    #[test]
    fn legacy_identity_gets_marker_and_rejects_a_checksummed_trailing_payload() {
        let root = tempfile::tempdir().unwrap();
        let mut settings = Settings::default();
        settings.node.data_dir = Some(root.path().to_owned());
        let identity = new_identity().unwrap();
        save(&root.path().join("IDENTITY"), &identity).unwrap();
        assert_eq!(
            NodeDirectory::open(&settings).unwrap().identity(),
            &identity
        );
        assert!(root.path().join("IDENTITY.initialized").is_file());
        let mut payload = postcard::to_stdvec(&identity).unwrap();
        payload.push(0);
        let mut bytes = b"FOCALND1".to_vec();
        bytes.extend_from_slice(blake3::hash(&payload).as_bytes());
        bytes.extend_from_slice(&payload);
        std::fs::write(root.path().join("IDENTITY"), bytes).unwrap();
        assert!(matches!(
            NodeDirectory::open(&settings),
            Err(NodeError::Identity)
        ));
    }
}
