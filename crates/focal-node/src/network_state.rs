//! Managed network bootstrap. Public genesis is pinned once; startup never
//! invents a replacement deployment beside an enrolled identity or durable WAL.
use crate::{
    config::Settings,
    embedded::{NodeError, NodeIdentity, atomic_file, read_bounded},
    node_directory::NodeDirectory,
};
use focal_control::{ControlBootstrap, ControlIdentity, ControlScope};
use focal_enrollment::{EnrollmentLimits, EnrollmentRegistry, EnrollmentRole};
use focal_model::{LedgerId, SessionId};
use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, path::Path, time::Duration};

const MAGIC: &[u8; 8] = b"FCLNET01";
// This is the immutable, one-founder genesis. Live directory/enrollment growth
// belongs to the replicated root checkpoint, never this startup manifest.
pub(crate) const MAX_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkGenesis {
    pub founder: NodeIdentity,
    pub root: ControlIdentity,
    pub root_namespace: LedgerId,
    pub bootstrap: ControlBootstrap,
}
impl NetworkGenesis {
    pub fn validate(&self, identity: &NodeIdentity) -> Result<(), NodeError> {
        self.validated_enrollment(identity).map(|_| ())
    }
    fn validated_enrollment(
        &self,
        identity: &NodeIdentity,
    ) -> Result<EnrollmentRegistry, NodeError> {
        identity.validate()?;
        self.founder.validate()?;
        if self.founder.schema != 1
            || self.founder.node == 0
            || self.founder.cluster != identity.cluster
            || self.founder.ledger != identity.ledger
            || self.founder.root != identity.root
            || self.founder.issuer != identity.issuer
            || self.founder.worker != identity.worker
            || self.founder.evaluator != identity.evaluator
            || self.root.cluster.0 != identity.cluster
            || self.root.scope != ControlScope::Root
            || self.root.group != root_group(identity.cluster)
            || self.root_namespace != root_namespace(identity)
            || self.root.genesis == [0; 32]
        {
            return Err(NodeError::Identity);
        }
        let registry = match &self.bootstrap {
            ControlBootstrap::Root {
                directory,
                enrollment,
            } if directory.cluster.0 == identity.cluster
                && directory.schema == 1
                && directory.revision == 0
                && directory.regions.is_empty()
                && directory.delegations.is_empty()
                && enrollment.len() <= focal_enrollment::MAX_MESSAGE_BYTES =>
            {
                EnrollmentRegistry::restore(
                    enrollment,
                    identity.cluster,
                    EnrollmentLimits::default(),
                )
                .map_err(|_| NodeError::Identity)?
            }
            _ => return Err(NodeError::Identity),
        };
        let mut enrollments = registry.enrollments();
        let founder = enrollments.next().ok_or(NodeError::Identity)?;
        if registry.revision() != 1
            || registry.applied_index() != 0
            || enrollments.next().is_some()
            || founder.identity.node_id != Some(self.founder.node)
            || founder.identity.principal != self.founder.issuer.0
            || founder.identity.role != EnrollmentRole::Node
            || registry
                .invitation_revoked(founder.invitation)
                .map_err(|_| NodeError::Identity)?
        {
            return Err(NodeError::Identity);
        }
        drop(enrollments);
        let options = focal_control::ControlOptions::new(focal_consensus::NodeConfig::single(
            self.founder.node,
            identity.cluster,
            self.root.group,
        ));
        if self
            .bootstrap
            .identity(&options)
            .map_err(|_| NodeError::Identity)?
            != self.root
        {
            return Err(NodeError::Identity);
        }
        Ok(registry)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkState {
    pub schema: u16,
    pub node: u64,
    pub listen: SocketAddr,
    pub advertise: SocketAddr,
    /// The enrollment endpoint remains pinned; this is not a redirect source.
    pub sponsor: focal_enrollment::ServerTrust,
    pub genesis: NetworkGenesis,
}
impl NetworkState {
    pub fn load(directory: &NodeDirectory) -> Result<Option<Self>, NodeError> {
        let root = directory.root();
        let path = root.join("NETWORK");
        if !path.exists() {
            if root.join("NETWORK.initialized").exists() {
                return Err(NodeError::Identity);
            }
            return Ok(None);
        }
        let bytes = read_bounded(&path, MAX_BYTES)?;
        let payload = bytes.get(40..).ok_or(NodeError::Identity)?;
        if bytes.get(..8) != Some(MAGIC.as_slice())
            || bytes.get(8..40) != Some(blake3::hash(payload).as_bytes().as_slice())
        {
            return Err(NodeError::Identity);
        }
        let (state, trailing): (Self, _) = postcard::take_from_bytes(payload)?;
        if !trailing.is_empty() {
            return Err(NodeError::Identity);
        }
        state.validate(directory.identity())?;
        Ok(Some(state))
    }
    pub fn install(&self, directory: &NodeDirectory) -> Result<(), NodeError> {
        self.validate(directory.identity())?;
        if let Some(existing) = Self::load(directory)? {
            if existing != *self {
                return Err(NodeError::Identity);
            }
        } else {
            let length = postcard::experimental::serialized_size(self)?;
            if length > MAX_BYTES.checked_sub(40).ok_or(NodeError::Identity)? {
                return Err(NodeError::Identity);
            }
            let payload = postcard::to_stdvec(self)?;
            let mut bytes = MAGIC.to_vec();
            bytes.extend_from_slice(blake3::hash(&payload).as_bytes());
            bytes.extend_from_slice(&payload);
            atomic_file(&directory.root().join("NETWORK"), &bytes)?;
        }
        atomic_file(
            &directory.root().join("NETWORK.initialized"),
            b"network identity installed",
        )?;
        Ok(())
    }
    pub fn validate(&self, identity: &NodeIdentity) -> Result<(), NodeError> {
        if self.schema != 1
            || self.node != identity.node
            || self.node == 0
            || !valid_listen(self.listen)
            || !valid_advertise(self.advertise)
            || self.sponsor.ca_certificate.is_empty()
            || self.sponsor.server_fingerprint == [0; 32]
            || self.sponsor.server_name.is_empty()
            || self.sponsor.endpoint.is_empty()
        {
            return Err(NodeError::Identity);
        }
        self.sponsor.validate().map_err(|_| NodeError::Identity)?;
        // Persist resolved reachability. A restart cannot silently select another
        // DNS result and disclose an invitation to an unpinned bootstrap host.
        let endpoint = self
            .sponsor
            .endpoint
            .parse::<SocketAddr>()
            .map_err(|_| NodeError::Identity)?;
        if !valid_advertise(endpoint) {
            return Err(NodeError::Identity);
        }
        let registry = self.genesis.validated_enrollment(identity)?;
        if registry.ca_certificate() != self.sponsor.ca_certificate {
            return Err(NodeError::Identity);
        }
        Ok(())
    }
    /// A persisted network node needs no repeated addresses or seed list.
    /// Changing an advertised identity requires a committed reachability update.
    pub async fn startup_addresses(
        &self,
        settings: &Settings,
    ) -> Result<(SocketAddr, SocketAddr), NodeError> {
        settings.validate()?;
        if settings.node.advertise.is_none() && settings.node.listen.is_none() {
            return Ok((self.listen, self.advertise));
        }
        let addresses = resolve_addresses(settings).await?;
        if addresses != (self.listen, self.advertise) {
            return Err(NodeError::Identity);
        }
        Ok(addresses)
    }
}
pub fn root_group(cluster: [u8; 16]) -> [u8; 16] {
    let mut hash = blake3::Hasher::new_derive_key("focal.cluster.root-group.v1");
    hash.update(&cluster);
    let mut group = [0; 16];
    for (target, source) in group.iter_mut().zip(hash.finalize().as_bytes()) {
        *target = *source;
    }
    group
}
pub fn root_namespace(identity: &NodeIdentity) -> LedgerId {
    LedgerId {
        tenant: identity.ledger.tenant,
        session: SessionId(root_group(identity.cluster)),
    }
}
pub async fn resolve_addresses(settings: &Settings) -> Result<(SocketAddr, SocketAddr), NodeError> {
    settings.validate()?;
    let advertised = settings
        .node
        .advertise
        .as_deref()
        .ok_or(NodeError::NetworkRequired)?;
    if advertised.len() > 1024 {
        return Err(NodeError::Identity);
    }
    let advertise = if let Ok(address) = advertised.parse::<SocketAddr>() {
        address
    } else {
        tokio::runtime::Handle::try_current()
            .map_err(|_| std::io::Error::other("endpoint resolution requires a Tokio runtime"))?;
        // Tokio's timer and resolver can panic when a caller supplied a runtime
        // without the corresponding driver, or the blocking pool cannot start.
        // Contain that dependency failure before it reaches the node owner.
        let lookup = async {
            tokio::time::timeout(Duration::from_secs(5), tokio::net::lookup_host(advertised))
                .await
                .map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "advertised endpoint resolution timed out",
                    )
                })?
        };
        std::panic::AssertUnwindSafe(lookup)
            .catch_unwind()
            .await
            .map_err(|_| std::io::Error::other("Tokio endpoint resolution failed"))??
            .next()
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::AddrNotAvailable,
                    "advertised endpoint resolved to no addresses",
                )
            })?
    };
    if !valid_advertise(advertise) {
        return Err(NodeError::Identity);
    }
    let listen = settings.node.listen.unwrap_or(advertise);
    if !valid_listen(listen) {
        return Err(NodeError::Identity);
    }
    Ok((listen, advertise))
}
pub fn network_requested(root: &Path, settings: &Settings) -> bool {
    settings.node.advertise.is_some()
        || settings.node.listen.is_some()
        || !settings.node.seeds.is_empty()
        || root.join("NETWORK").exists()
        || root.join("NETWORK.initialized").exists()
        || root.join("JOIN").exists()
        || root.join("JOIN.initialized").exists()
        || root.join("cluster/network").exists()
}
fn valid_listen(address: SocketAddr) -> bool {
    address.port() != 0
        && !address.ip().is_multicast()
        && !matches!(address.ip(), std::net::IpAddr::V4(ip) if ip.is_broadcast())
}
fn valid_advertise(address: SocketAddr) -> bool {
    valid_listen(address) && !address.ip().is_unspecified()
}
