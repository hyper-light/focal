//! Private preparation of the first node's enrollment in a new cluster genesis.
use crate::{files::PrivateDirectory, *};
use serde::{Deserialize, Serialize};
use std::path::Path;
use zeroize::Zeroizing;

#[derive(Serialize, Deserialize)]
struct SavedFounder {
    schema: u16,
    registry: Vec<u8>,
    receipt: EnrollmentReceipt,
}
/// A provisional NEW genesis, never an update to an established registry.
/// The process owner must durably commit/install this exact public registry
/// through its root metadata authority before granting the certificate access
/// or calling JoinKey::complete. This object contains no remotely deliverable
/// invitation and confers no voter membership on its own.
pub struct FoundingEnrollmentDraft {
    registry: EnrollmentRegistry,
    receipt: EnrollmentReceipt,
    _directory: PrivateDirectory,
}
impl FoundingEnrollmentDraft {
    /// Persists the exact initial certificate/registry before its first proposal,
    /// so retries cannot replace a certificate after a lost genesis response.
    /// This constructor has no existing-registry argument: replacing an already
    /// installed genesis is never an enrollment operation supported by this API.
    pub fn open_or_create(
        path: impl AsRef<Path>,
        authority: &BootstrapAuthority,
        key: &JoinKey,
        node: u64,
        principal: [u8; 16],
        limits: EnrollmentLimits,
        now: i64,
    ) -> Result<Self, EnrollmentError> {
        if node == 0 || principal == [0; 16] || key.cluster() != authority.cluster() {
            return Err(EnrollmentError::WrongCluster);
        }
        let directory = PrivateDirectory::open(path.as_ref())?;
        let (registry, receipt) = match directory.read("founder.bin")? {
            Some(bytes) => {
                let saved: SavedFounder = decode(&bytes)?;
                if saved.schema != 1 {
                    return Err(EnrollmentError::Corrupt);
                }
                let registry =
                    EnrollmentRegistry::restore(&saved.registry, authority.cluster(), limits)?;
                (registry, saved.receipt)
            }
            None => {
                let (registry, receipt) =
                    EnrollmentRegistry::founding(authority, key, node, principal, limits, now)?;
                let saved = SavedFounder {
                    schema: 1,
                    registry: registry.checkpoint()?,
                    receipt: receipt.clone(),
                };
                directory.install_new("founder.bin", &Zeroizing::new(encode(&saved)?))?;
                (registry, receipt)
            }
        };
        if registry.ca_certificate() != authority.ca_certificate() {
            return Err(EnrollmentError::WrongCluster);
        }
        if registry.revision() != 1
            || registry.applied_index() != 0
            || registry.enrollments().count() != 1
            || registry.enrollments().next() != Some(&receipt)
            || receipt.identity.cluster != authority.cluster()
            || receipt.identity.role != EnrollmentRole::Node
            || receipt.identity.node_id != Some(node)
            || receipt.identity.principal != principal
            || receipt.request != key.request_id()
            || receipt.public_key != crate::pki::csr_key_hash(key.csr())?
            || receipt.csr_hash != hash("focal.enrollment.csr.v1", key.csr())
        {
            return Err(EnrollmentError::Conflict);
        }
        crate::pki::verify_issued(&receipt, authority.ca_certificate())?;
        Ok(Self {
            registry,
            receipt,
            _directory: directory,
        })
    }
    pub fn registry(&self) -> &EnrollmentRegistry {
        &self.registry
    }
    /// Public but provisional. Network authorization still requires the owning
    /// root authority's committed genesis; private key delivery is unnecessary.
    pub fn receipt(&self) -> &EnrollmentReceipt {
        &self.receipt
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn now() -> i64 {
        1_783_000_000
    }
    #[test]
    fn founder_is_bound_to_original_identity_and_exact_genesis_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let authority = BootstrapAuthority::open_or_create(
            dir.path().join("ca"),
            [5; 16],
            vec!["founder.internal".into()],
            now(),
        )
        .unwrap();
        let key = JoinKey::open_or_create(dir.path().join("key"), [5; 16]).unwrap();
        let path = dir.path().join("founder");
        let draft = FoundingEnrollmentDraft::open_or_create(
            &path,
            &authority,
            &key,
            41,
            [6; 16],
            EnrollmentLimits::default(),
            now(),
        )
        .unwrap();
        let receipt = draft.receipt().clone();
        let checkpoint = draft.registry().checkpoint().unwrap();
        assert_eq!(receipt.identity.node_id, Some(41));
        assert_eq!(receipt.identity.principal, [6; 16]);
        assert_eq!(
            draft
                .registry()
                .authorize_certificate(&receipt.certificate, now())
                .unwrap(),
            receipt.identity
        );
        drop(draft);
        let retry = FoundingEnrollmentDraft::open_or_create(
            &path,
            &authority,
            &key,
            41,
            [6; 16],
            EnrollmentLimits::default(),
            now() + 1,
        )
        .unwrap();
        assert_eq!(retry.receipt(), &receipt);
        assert_eq!(retry.registry().checkpoint().unwrap(), checkpoint);
        let restored =
            EnrollmentRegistry::restore(&checkpoint, [5; 16], EnrollmentLimits::default()).unwrap();
        assert_eq!(restored.enrollments().next(), Some(&receipt));
        // The owner may complete local key custody only after installing genesis.
        let material = key
            .complete(&receipt, authority.ca_certificate(), now())
            .unwrap();
        assert_eq!(
            material.certificate_chain().first(),
            Some(&receipt.certificate)
        );
        drop(retry);
        assert!(matches!(
            FoundingEnrollmentDraft::open_or_create(
                &path,
                &authority,
                &key,
                42,
                [6; 16],
                EnrollmentLimits::default(),
                now()
            ),
            Err(EnrollmentError::Conflict)
        ));
        assert!(matches!(
            FoundingEnrollmentDraft::open_or_create(
                &path,
                &authority,
                &key,
                41,
                [7; 16],
                EnrollmentLimits::default(),
                now()
            ),
            Err(EnrollmentError::Conflict)
        ));
        let other = JoinKey::open_or_create(dir.path().join("other-key"), [5; 16]).unwrap();
        assert!(matches!(
            FoundingEnrollmentDraft::open_or_create(
                &path,
                &authority,
                &other,
                41,
                [6; 16],
                EnrollmentLimits::default(),
                now()
            ),
            Err(EnrollmentError::Conflict)
        ));
        std::fs::remove_file(path.join("founder.bin")).unwrap();
        assert!(matches!(
            FoundingEnrollmentDraft::open_or_create(
                &path,
                &authority,
                &key,
                41,
                [6; 16],
                EnrollmentLimits::default(),
                now()
            ),
            Err(EnrollmentError::Corrupt)
        ));
    }
    #[test]
    fn founder_rejects_wrong_cluster_and_exhausted_node_counter() {
        let dir = tempfile::tempdir().unwrap();
        let authority = BootstrapAuthority::open_or_create(
            dir.path().join("ca"),
            [5; 16],
            vec!["founder.internal".into()],
            now(),
        )
        .unwrap();
        let wrong = JoinKey::open_or_create(dir.path().join("wrong"), [8; 16]).unwrap();
        assert!(matches!(
            FoundingEnrollmentDraft::open_or_create(
                dir.path().join("founder"),
                &authority,
                &wrong,
                41,
                [6; 16],
                EnrollmentLimits::default(),
                now()
            ),
            Err(EnrollmentError::WrongCluster)
        ));
        let key = JoinKey::open_or_create(dir.path().join("key"), [5; 16]).unwrap();
        assert!(matches!(
            FoundingEnrollmentDraft::open_or_create(
                dir.path().join("overflow"),
                &authority,
                &key,
                u64::MAX,
                [6; 16],
                EnrollmentLimits::default(),
                now()
            ),
            Err(EnrollmentError::Capacity)
        ));
    }
}
