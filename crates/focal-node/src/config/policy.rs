//! The committed policy of a store (doc 08 §2): the durability and
//! placement intent pinned when the store was created and changed since
//! only by plan and apply, kept in the data directory's `POLICY` file with
//! a revision and a hash. A missing policy beside an existing store is
//! data loss, never permission to choose a new guarantee.
use super::{ConfigError, Durability, Placement};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// `FCLPOL2`: the committed policy with its revision; the original file
/// held the bare `(durability, placement)` pair, which reads as revision 1.
const MAGIC: &[u8; 8] = b"FCLPOL2\0";
const MAX_BYTES: usize = 64 * 1024;

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

fn policy_path(root: &Path) -> std::path::PathBuf {
    root.join("POLICY")
}
fn marker_path(root: &Path) -> std::path::PathBuf {
    root.join("POLICY.initialized")
}
fn read_bounded(path: &Path) -> Result<Vec<u8>, ConfigError> {
    use std::io::Read as _;
    let mut file = std::fs::File::open(path)
        .map_err(|error| ConfigError::PolicyEncoding(error.to_string()))?;
    let mut bytes = Vec::new();
    file.by_ref()
        .take((MAX_BYTES as u64).saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|error| ConfigError::PolicyEncoding(error.to_string()))?;
    if bytes.len() > MAX_BYTES {
        return Err(ConfigError::PolicyEncoding("policy file too large".into()));
    }
    Ok(bytes)
}
/// The committed policy of the store under `root`, `None` before one is
/// pinned. A marker without a file is a lost policy.
pub fn read_committed(root: &Path) -> Result<Option<CommittedPolicy>, ConfigError> {
    let path = policy_path(root);
    if path.exists() {
        return CommittedPolicy::decode(&read_bounded(&path)?).map(Some);
    }
    if marker_path(root).exists() {
        return Err(ConfigError::PolicyMissing);
    }
    Ok(None)
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
    root: &Path,
    intent: &PolicyIntent,
    write: impl Fn(&Path, &[u8]) -> Result<(), std::io::Error>,
) -> Result<CommittedPolicy, ConfigError> {
    let path = policy_path(root);
    let marker = marker_path(root);
    match read_committed(root)? {
        Some(committed) => {
            if let Some(field) = committed.intent.differing_field(intent) {
                return Err(ConfigError::CommittedPolicyChange { field });
            }
            if !marker.exists() {
                write(&marker, b"deployment policy installed")
                    .map_err(|error| ConfigError::PolicyEncoding(error.to_string()))?;
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
            write(&path, &committed.encode()?)
                .map_err(|error| ConfigError::PolicyEncoding(error.to_string()))?;
            write(&marker, b"deployment policy installed")
                .map_err(|error| ConfigError::PolicyEncoding(error.to_string()))?;
            Ok(committed)
        }
    }
}
/// Commit a new revision of the policy (plan and apply, doc 08 §9): the
/// next revision of the current one, written atomically by `write`.
pub fn commit(
    root: &Path,
    intent: &PolicyIntent,
    expected: PolicyRevision,
    write: impl FnOnce(&Path, &[u8]) -> Result<(), std::io::Error>,
) -> Result<CommittedPolicy, ConfigError> {
    let current = read_committed(root)?.ok_or(ConfigError::PolicyMissing)?;
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
    write(&policy_path(root), &committed.encode()?)
        .map_err(|error| ConfigError::PolicyEncoding(error.to_string()))?;
    Ok(committed)
}
