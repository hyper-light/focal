//! The committed policy of a store (doc 08 §2): the durability and
//! placement intent pinned when the store was created and changed since
//! only by plan and apply, kept in the data directory's `POLICY` file with
//! a revision and a hash. A missing policy beside an existing store is
//! data loss, never permission to choose a new guarantee. The file is one
//! of the node's small files, sealed from the at-rest level (29 §12.10).
use super::{ConfigError, Durability, Placement};
use crate::node_files::NodeFiles;
use serde::{Deserialize, Serialize};
use std::path::Path;

/// `FCLPOL2`: the committed policy with its revision; the original file
/// held the bare `(durability, placement)` pair, which reads as revision 1.
const MAGIC: &[u8; 8] = b"FCLPOL2\0";
/// Derived: the most a `POLICY` file holds: an intent's durability and placement, its revision and
/// hash, its regions and residency at their configured bounds, within 64 KiB.
pub(crate) const MAX_BYTES: usize = 64 * 1024;
/// The node file the committed policy is kept in.
const POLICY: &str = "POLICY";

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyIntent {
    pub durability: Durability,
    pub placement: Placement,
}
impl PolicyIntent {
    /// The first field that differs from `other`, by its configuration path.
    pub fn differing_field(&self, other: &Self) -> Option<&'static str> {
        if self.durability.survive != other.durability.survive {
            Some("durability.survive")
        } else if self.durability.max_failures != other.durability.max_failures {
            Some("durability.max_failures")
        } else if self.placement.home_regions != other.placement.home_regions {
            Some("placement.home_regions")
        } else if self.placement.residency != other.placement.residency {
            Some("placement.residency")
        } else {
            None
        }
    }
    /// A stable identity of the intent: the hash of its canonical encoding.
    pub fn hash(&self) -> Result<[u8; 32], ConfigError> {
        let bytes = postcard::to_stdvec(&(&self.durability, &self.placement))
            .map_err(|error| ConfigError::PolicyEncoding(error.to_string()))?;
        Ok(*blake3::hash(&bytes).as_bytes())
    }
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PolicyRevision(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedPolicy {
    pub revision: PolicyRevision,
    pub intent: PolicyIntent,
    pub hash: [u8; 32],
}
impl CommittedPolicy {
    fn encode(&self) -> Result<Vec<u8>, ConfigError> {
        let body = postcard::to_stdvec(self)
            .map_err(|error| ConfigError::PolicyEncoding(error.to_string()))?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(MAGIC.len().saturating_add(body.len()))
            .map_err(|_| ConfigError::PolicyEncoding("allocation".into()))?;
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&body);
        Ok(bytes)
    }
    /// Decode a `POLICY` file: the revisioned form, or the original pair.
    pub fn decode(bytes: &[u8]) -> Result<Self, ConfigError> {
        if bytes.len() > MAX_BYTES {
            return Err(ConfigError::PolicyEncoding("policy file too large".into()));
        }
        if let Some(body) = bytes.strip_prefix(MAGIC.as_slice()) {
            let (policy, rest): (Self, &[u8]) = postcard::take_from_bytes(body)
                .map_err(|error| ConfigError::PolicyEncoding(error.to_string()))?;
            if !rest.is_empty() || policy.revision.0 == 0 || policy.intent.hash()? != policy.hash {
                return Err(ConfigError::PolicyEncoding(
                    "policy revision, hash or trailing bytes".into(),
                ));
            }
            return Ok(policy);
        }
        let ((durability, placement), rest): ((Durability, Placement), &[u8]) =
            postcard::take_from_bytes(bytes)
                .map_err(|error| ConfigError::PolicyEncoding(error.to_string()))?;
        if !rest.is_empty() {
            return Err(ConfigError::PolicyEncoding("trailing bytes".into()));
        }
        let intent = PolicyIntent {
            durability,
            placement,
        };
        Ok(Self {
            revision: PolicyRevision(1),
            hash: intent.hash()?,
            intent,
        })
    }
}

fn marker_path(root: &Path) -> std::path::PathBuf {
    root.join("POLICY.initialized")
}
/// Whether `bytes` are a `POLICY` file's, as [`CommittedPolicy::decode`] holds them to: what a
/// conversion of the node's files checks before it seals one.
pub(crate) fn check(bytes: &[u8]) -> Result<(), crate::embedded::NodeError> {
    CommittedPolicy::decode(bytes)
        .map(|_| ())
        .map_err(crate::embedded::NodeError::from)
}
/// The committed policy of the store whose node files are `files`, `None`
/// before one is pinned. A marker without a file is a lost policy.
pub fn read_committed(files: &NodeFiles) -> Result<Option<CommittedPolicy>, ConfigError> {
    match files
        .read(POLICY, MAX_BYTES)
        .map_err(|error| ConfigError::PolicyEncoding(error.to_string()))?
    {
        Some(bytes) => CommittedPolicy::decode(&bytes).map(Some),
        None if marker_path(files.root()).exists() => Err(ConfigError::PolicyMissing),
        None => Ok(None),
    }
}
/// [`read_committed`] of the data directory `root` beside its running owner, or before one starts:
/// its node files opened under the keys `settings` finds for it, made nowhere.
pub fn read_committed_in(
    settings: &super::Settings,
    root: &Path,
) -> Result<Option<CommittedPolicy>, ConfigError> {
    let keys = settings
        .found_data_keys(root)
        .map_err(|error| ConfigError::PolicyEncoding(error.to_string()))?;
    read_committed(&NodeFiles::new(root, keys))
}
/// Whether the directory already holds a store the policy must have been
/// pinned for.
fn store_exists(root: &Path) -> bool {
    ["wal", "content", "NETWORK", "NETWORK.initialized"]
        .iter()
        .any(|name| root.join(name).exists())
}
/// Pin `intent` for a new store, or check it against the committed policy
/// of an existing one: a difference is refused by the field that differs,
/// never overwritten. Existing policy bytes keep their original encoding.
pub fn install_or_check(
    files: &NodeFiles,
    intent: &PolicyIntent,
) -> Result<CommittedPolicy, ConfigError> {
    let root = files.root();
    let marker = marker_path(root);
    let write_marker = || {
        crate::embedded::atomic_file(&marker, b"deployment policy installed")
            .map_err(|error| ConfigError::PolicyEncoding(error.to_string()))
    };
    match read_committed(files)? {
        Some(committed) => {
            if let Some(field) = committed.intent.differing_field(intent) {
                return Err(ConfigError::CommittedPolicyChange { field });
            }
            if !marker.exists() {
                write_marker()?;
            }
            Ok(committed)
        }
        None => {
            if store_exists(root) {
                return Err(ConfigError::PolicyMissing);
            }
            let committed = CommittedPolicy {
                revision: PolicyRevision(1),
                hash: intent.hash()?,
                intent: intent.clone(),
            };
            files
                .write(POLICY, &committed.encode()?)
                .map_err(|error| ConfigError::PolicyEncoding(error.to_string()))?;
            write_marker()?;
            Ok(committed)
        }
    }
}
/// Commit a new revision of the policy (plan and apply, doc 08 §9): the
/// next revision of the current one, written atomically by `write`.
pub fn commit(
    files: &NodeFiles,
    intent: &PolicyIntent,
    expected: PolicyRevision,
) -> Result<CommittedPolicy, ConfigError> {
    let current = read_committed(files)?.ok_or(ConfigError::PolicyMissing)?;
    if current.revision != expected {
        return Err(ConfigError::PolicyEncoding(format!(
            "policy revision {} is not the expected {}",
            current.revision.0, expected.0
        )));
    }
    let committed = CommittedPolicy {
        revision: PolicyRevision(current.revision.0.saturating_add(1)),
        hash: intent.hash()?,
        intent: intent.clone(),
    };
    files
        .write(POLICY, &committed.encode()?)
        .map_err(|error| ConfigError::PolicyEncoding(error.to_string()))?;
    Ok(committed)
}
