//! The typed, versioned deployment configuration and its ownership (doc 08
//! §2): `node` and `topology` are local deployment facts a startup may
//! override; `durability` and `placement` are committed policy intent that
//! a running store changes only through plan and apply. One schema, two
//! authorities, every value's source named.
pub mod local;
pub mod policy;
pub mod resolve;
pub mod schema;
#[cfg(test)]
mod ownership_tests {
    include!("tests.rs");
}
pub use local::LocalFacts;
pub use policy::{CommittedPolicy, PolicyIntent, PolicyRevision};
pub use resolve::{
    CliOverrides, ConfigSource, FieldSources, ResolvedSettings, resolve, resolve_request,
};
pub use schema::{SCHEMA, check_unknown_keys};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;

pub const CONFIG_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    pub version: u16,
    #[serde(default)]
    pub node: NodeSettings,
    #[serde(default)]
    pub topology: Topology,
    #[serde(default)]
    pub durability: Durability,
    #[serde(default)]
    pub placement: Placement,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            version: CONFIG_VERSION,
            node: NodeSettings::default(),
            topology: Topology::default(),
            durability: Durability::default(),
            placement: Placement::default(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NodeSettings {
    pub data_dir: Option<PathBuf>,
    pub listen: Option<SocketAddr>,
    pub advertise: Option<String>,
    pub seeds: Vec<String>,
    /// Tenants this node hosts sessions for at most, its own included;
    /// default 8, at most 1024. A placement that would need one more is
    /// refused by this node as `NodeCapacity`.
    pub max_tenants: Option<usize>,
    /// An optional loopback endpoint for the read-only metrics text (doc
    /// 08 §9); node-local, never a cluster fact.
    pub metrics_listen: Option<SocketAddr>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Topology {
    pub zone: Option<String>,
    pub region: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureDomain {
    #[default]
    Node,
    Zone,
    Region,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Durability {
    pub survive: FailureDomain,
    pub max_failures: u16,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Placement {
    pub home_regions: Vec<String>,
    pub residency: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("configuration does not match schema: {0}")]
    Parse(#[from] serde_saphyr::Error),
    #[error("unsupported configuration version {0}")]
    Version(u16),
    #[error("configuration field {field}: {reason}")]
    Invalid {
        field: &'static str,
        reason: &'static str,
    },
    #[error("no OS application-data directory is available; specify --data-dir")]
    NoDataDirectory,
    #[error("unknown configuration key `{path}`")]
    UnknownKey { path: String },
    #[error(
        "configuration field {field} differs from the committed policy of this store; commit the change through `deployment plan` and `deployment apply`, or start with the committed value"
    )]
    CommittedPolicyChange { field: &'static str },
    #[error(
        "the committed policy of this store is missing or unreadable; it is not recreated beside an existing store"
    )]
    PolicyMissing,
    #[error("committed policy: {0}")]
    PolicyEncoding(String),
}

impl Settings {
    pub fn from_yaml(bytes: &str) -> Result<Self, ConfigError> {
        if bytes.len() > 64 * 1024 {
            return Err(ConfigError::Invalid {
                field: "configuration",
                reason: "exceeds 64 KiB",
            });
        }
        let options = serde_saphyr::options! {
            budget: serde_saphyr::budget! {max_depth:16,max_events:8192,max_nodes:4096,max_total_scalar_bytes:64*1024,max_aliases:0,max_anchors:0,max_documents:1},
        };
        let settings: Self = serde_saphyr::from_str_with_options(bytes, options)?;
        settings.validate()?;
        Ok(settings)
    }
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.version != CONFIG_VERSION {
            return Err(ConfigError::Version(self.version));
        }
        if self
            .node
            .advertise
            .as_ref()
            .is_some_and(|s| s.trim().is_empty())
        {
            return Err(ConfigError::Invalid {
                field: "node.advertise",
                reason: "must be a reachable nonempty endpoint",
            });
        }
        if self
            .node
            .data_dir
            .as_ref()
            .is_some_and(|s| s.as_os_str().is_empty())
        {
            return Err(ConfigError::Invalid {
                field: "node.data_dir",
                reason: "must not be empty",
            });
        }
        if self.node.seeds.iter().any(|s| s.trim().is_empty())
            || self
                .node
                .seeds
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != self.node.seeds.len()
        {
            return Err(ConfigError::Invalid {
                field: "node.seeds",
                reason: "must contain unique nonempty endpoints",
            });
        }
        if self.node.max_tenants.is_some_and(|count| {
            count == 0 || count > crate::admission::AdmissionPolicy::MAX_TENANTS
        }) {
            return Err(ConfigError::Invalid {
                field: "node.max_tenants",
                reason: "must be between 1 and 1024",
            });
        }
        if self
            .node
            .metrics_listen
            .is_some_and(|address| !address.ip().is_loopback())
        {
            return Err(ConfigError::Invalid {
                field: "node.metrics_listen",
                reason: "must be a loopback address; metrics are read-only and unauthenticated",
            });
        }
        for (field, value) in [
            ("topology.zone", &self.topology.zone),
            ("topology.region", &self.topology.region),
        ] {
            if value.as_ref().is_some_and(|s| s.trim().is_empty()) {
                return Err(ConfigError::Invalid {
                    field,
                    reason: "must not be empty",
                });
            }
        }
        for (field, regions) in [
            ("placement.home_regions", &self.placement.home_regions),
            ("placement.residency", &self.placement.residency),
        ] {
            let set: std::collections::BTreeSet<_> = regions.iter().collect();
            if set.len() != regions.len() || regions.iter().any(|s| s.trim().is_empty()) {
                return Err(ConfigError::Invalid {
                    field,
                    reason: "must contain unique nonempty region names",
                });
            }
        }
        if !self.placement.residency.is_empty()
            && self
                .placement
                .home_regions
                .iter()
                .any(|r| !self.placement.residency.contains(r))
        {
            return Err(ConfigError::Invalid {
                field: "placement.home_regions",
                reason: "ordering homes must lie inside the hard residency boundary",
            });
        }
        Ok(())
    }

    /// The local deployment facts of this configuration.
    pub fn local_facts(&self) -> LocalFacts {
        LocalFacts {
            node: self.node.clone(),
            topology: self.topology.clone(),
        }
    }
    /// The policy intent of this configuration.
    pub fn policy_intent(&self) -> PolicyIntent {
        PolicyIntent {
            durability: self.durability.clone(),
            placement: self.placement.clone(),
        }
    }
    pub fn data_dir(&self) -> Result<PathBuf, ConfigError> {
        if let Some(path) = &self.node.data_dir {
            return Ok(path.clone());
        }
        if cfg!(target_os = "macos") {
            return std::env::var_os("HOME")
                .map(|h| PathBuf::from(h).join("Library/Application Support/Focal"))
                .ok_or(ConfigError::NoDataDirectory);
        }
        if let Some(path) = std::env::var_os("XDG_DATA_HOME") {
            return Ok(PathBuf::from(path).join("focal"));
        }
        std::env::var_os("HOME")
            .map(|h| PathBuf::from(h).join(".local/share/focal"))
            .ok_or(ConfigError::NoDataDirectory)
    }
}

/// Patch semantics preserve omitted committed values; startup defaults never reset a
/// live deployment's guarantee. Node-local fields cannot be changed by this policy patch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyPatch {
    pub version: u16,
    pub durability: Option<DurabilityPatch>,
    pub placement: Option<PlacementPatch>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurabilityPatch {
    pub survive: Option<FailureDomain>,
    pub max_failures: Option<u16>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlacementPatch {
    pub home_regions: Option<Vec<String>>,
    pub residency: Option<Vec<String>>,
}

impl PolicyPatch {
    pub fn apply(&self, existing: &Settings) -> Result<Settings, ConfigError> {
        if self.version != CONFIG_VERSION {
            return Err(ConfigError::Version(self.version));
        }
        let mut next = existing.clone();
        if let Some(d) = &self.durability {
            if let Some(s) = d.survive {
                next.durability.survive = s;
            }
            if let Some(f) = d.max_failures {
                next.durability.max_failures = f;
            }
        }
        if let Some(p) = &self.placement {
            if let Some(r) = &p.home_regions {
                next.placement.home_regions = r.clone();
            }
            if let Some(r) = &p.residency {
                next.placement.residency = r.clone();
            }
        }
        next.validate()?;
        Ok(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn local_defaults_do_not_claim_replication() {
        assert_eq!(
            Settings::from_yaml("version: 1").unwrap(),
            Settings::default()
        );
        assert!(Settings::from_yaml("version: 1\nshards: 3").is_err());
        assert!(Settings::from_yaml("version: 1\nnode:\n  shards: 3").is_err());
    }
    #[test]
    fn the_tenant_bound_is_optional_and_bounded() {
        assert_eq!(Settings::default().node.max_tenants, None);
        assert_eq!(
            Settings::from_yaml("version: 1\nnode:\n  max_tenants: 3")
                .unwrap()
                .node
                .max_tenants,
            Some(3)
        );
        for bad in ["0", "1025", "-1", "many"] {
            assert!(
                Settings::from_yaml(&format!("version: 1\nnode:\n  max_tenants: {bad}")).is_err(),
                "{bad}"
            );
        }
    }
    #[test]
    fn omitted_policy_does_not_reset_a_committed_guarantee() {
        let old=Settings::from_yaml("version: 1\ndurability:\n  survive: region\n  max_failures: 1\nplacement:\n  residency: [a, b, c]").unwrap();
        let p: PolicyPatch =
            serde_saphyr::from_str("version: 1\nplacement:\n  home_regions: [a]").unwrap();
        let next = p.apply(&old).unwrap();
        assert_eq!(next.durability, old.durability);
        assert_eq!(next.placement.residency, old.placement.residency);
        let bad: PolicyPatch =
            serde_saphyr::from_str("version: 1\nplacement:\n  home_regions: [elsewhere]").unwrap();
        assert!(bad.apply(&old).is_err());
    }
}
