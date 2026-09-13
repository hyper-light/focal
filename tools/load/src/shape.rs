//! The workload description (`--shape` YAML). A minimal first version: a count
//! of native claim creations and a deterministic seed. It is intentionally
//! extensible — reads, concurrency, evidence size and transport belong here as
//! the generator grows (R11 §5).
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkloadShape {
    /// Total native claim creations to submit end to end.
    pub claims: u64,
    /// Deterministic seed for request and claim identities, so a run is
    /// reproducible and two runs never collide.
    #[serde(default = "default_seed")]
    pub seed: u64,
}

fn default_seed() -> u64 {
    1
}

impl WorkloadShape {
    pub fn validate(&self) -> Result<(), String> {
        if self.claims == 0 {
            return Err("claims must be greater than zero".to_string());
        }
        if self.claims > 1_000_000 {
            return Err("claims must not exceed 1_000_000".to_string());
        }
        Ok(())
    }
}
