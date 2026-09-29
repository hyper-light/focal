//! Deployment plans and their resumable application (doc 08 §9).
//!
//! A plan is an immutable artifact computed from what the node observes
//! (committed policy revision, the directory's sessions with their epochs)
//! and what the operator requests (the configuration file). Planning is
//! read-only: per-session proposals are dry runs the placement agent answers
//! without journaling. Applying a plan rechecks every observation it was
//! built on and refuses a stale plan before any side effect; progress is
//! journaled per change so a repeated `apply` resumes, never repeats.
pub mod apply;
pub mod observe;
pub mod plan;
pub mod render;
#[cfg(test)]
mod tests;

use crate::config::{ConfigError, FailureDomain};
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum DeploymentError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Admin(#[from] crate::cluster_admin::ClusterAdminError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("deployment artifact encoding: {0}")]
    Encoding(String),
    #[error("deployment artifact is corrupt: {0}")]
    Corrupt(&'static str),
    #[error("plan was made for another deployment")]
    WrongDeployment,
    #[error("plan is stale: {subject} changed its {field}")]
    Stale {
        subject: String,
        field: &'static str,
    },
    #[error("plan identifies missing capacity for {0} session(s); the existing contract stays")]
    Blocked(usize),
    #[error("the directory view is truncated; the plan cannot name every session")]
    Truncated,
    #[error(
        "the directory has not reported this node yet: it is still starting, or it runs no directory; plan from a node that does, once `cluster placement` shows it"
    )]
    NotObserved,
    #[error("deployment plan needs the requested configuration (--config FILE)")]
    NoConfig,
    #[error("requested policy is unsatisfiable here: {0}")]
    Unsatisfiable(#[from] crate::placement::PlacementError),
    #[error("deployment artifact exceeds its bound")]
    Capacity,
    #[error("render: {0}")]
    Render(String),
}
impl DeploymentError {
    pub fn classification(&self) -> focal_client::failure::Failure {
        use focal_client::failure::Failure;
        match self {
            Self::Stale { .. } => Failure {
                condition: "Stale",
                code: "stale_plan",
                exit_code: 5,
            },
            Self::WrongDeployment => Failure::error("wrong_deployment", 2),
            Self::Corrupt(_) => Failure::error("plan_corrupt", 2),
            Self::Encoding(_) => Failure::error("plan_encoding", 1),
            Self::Blocked(_) | Self::Unsatisfiable(_) => Failure {
                condition: "GuaranteeUnsatisfied",
                code: "guarantee_unsatisfied",
                exit_code: 6,
            },
            Self::Truncated | Self::Capacity => Failure::error("capacity", 6),
            Self::NotObserved => Failure {
                condition: "NotReady",
                code: "not_observed",
                exit_code: 6,
            },
            Self::NoConfig | Self::Render(_) => Failure::error("invalid_input", 2),
            Self::Config(_) | Self::Admin(_) | Self::Io(_) => Failure::error("deployment", 1),
        }
    }
}

/// A durability level: what a guarantee tolerates. Ordered by tolerated
/// failures first, then by the breadth of the failure domain, so `node/1`
/// ranks above `zone/0` (which tolerates no loss at all).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct GuaranteeLevel {
    pub survive: FailureDomain,
    pub max_failures: u16,
}
impl GuaranteeLevel {
    pub const NONE: Self = Self {
        survive: FailureDomain::Node,
        max_failures: 0,
    };
    fn rank(self) -> (u16, u8) {
        (
            self.max_failures,
            match self.survive {
                FailureDomain::Node => 0,
                FailureDomain::Zone => 1,
                FailureDomain::Region => 2,
            },
        )
    }
    pub fn weaker(self, other: Self) -> Self {
        if other.rank() < self.rank() {
            other
        } else {
            self
        }
    }
    /// Whether this level provides at least `required`.
    pub fn covers(self, required: Self) -> bool {
        self.rank() >= required.rank()
    }
}
/// The guarantee a plan carries: before it, while it runs (the old contract
/// holds until the new placement is verified) and once it completes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Guarantee {
    pub before: GuaranteeLevel,
    pub during: GuaranteeLevel,
    pub after: GuaranteeLevel,
}

/// The directory names failure classes by their debug form (`Node`); the
/// configuration by its snake case (`node`). Both parse here.
pub fn parse_survive(name: &str) -> Option<FailureDomain> {
    match name.to_ascii_lowercase().as_str() {
        "node" => Some(FailureDomain::Node),
        "zone" => Some(FailureDomain::Zone),
        "region" => Some(FailureDomain::Region),
        _ => None,
    }
}
pub fn survive_name(survive: FailureDomain) -> &'static str {
    match survive {
        FailureDomain::Node => "node",
        FailureDomain::Zone => "zone",
        FailureDomain::Region => "region",
    }
}
pub fn survive_code(survive: FailureDomain) -> u8 {
    match survive {
        FailureDomain::Node => 0,
        FailureDomain::Zone => 1,
        FailureDomain::Region => 2,
    }
}
pub(crate) fn hex(bytes: &[u8]) -> String {
    let mut text = String::new();
    if text
        .try_reserve_exact(bytes.len().saturating_mul(2))
        .is_err()
    {
        return text;
    }
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(text, "{byte:02x}");
    }
    text
}
pub fn now_ms() -> Result<u64, DeploymentError> {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| DeploymentError::Corrupt("system clock before the epoch"))?;
    u64::try_from(elapsed.as_millis()).map_err(|_| DeploymentError::Capacity)
}
