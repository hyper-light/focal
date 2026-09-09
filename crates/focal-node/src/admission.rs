//! Tenant admission: which tenants a node hosts sessions for, and the memory
//! each of them may draw from the node's budget. A tenant is admitted when a
//! committed placement first assigns one of its sessions to this node, under
//! a fixed allowance carved from the node budget, up to the operator's
//! `node.max_tenants`; a node that cannot take another tenant refuses the
//! assignment as `NodeCapacity` in the directory rather than hosting it
//! unfunded. Every tenant then queues through the fleet's fair scheduler
//! under its own quota, so a noisy tenant's ordinary work is throttled by its
//! allowance and never by another tenant's completion reserve.
use crate::fleet::FleetTenant;
use focal_directory::QueueUsage;
use focal_memory::{DiskStats, MemoryBudget};
use focal_model::TenantId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The fixed allowance every admitted tenant receives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmissionPolicy {
    /// Tenants this node hosts at most, the founder's included.
    pub max_tenants: usize,
    /// The memory allowance of one tenant; the node budget still arbitrates
    /// live use between tenants.
    pub tenant_memory: usize,
    pub tenant_completion_reserve: usize,
    /// The scheduler weight of one tenant.
    pub weight: u32,
}
impl AdmissionPolicy {
    pub const DEFAULT_MAX_TENANTS: usize = 8;
    pub const MAX_TENANTS: usize = 1024;
    /// The standard allowance under the operator's tenant bound.
    pub fn standard(max_tenants: Option<usize>) -> Self {
        Self {
            max_tenants: max_tenants
                .unwrap_or(Self::DEFAULT_MAX_TENANTS)
                .clamp(1, Self::MAX_TENANTS),
            tenant_memory: 512 * 1024 * 1024,
            tenant_completion_reserve: 128 * 1024 * 1024,
            weight: 1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AdmissionRefusal {
    #[error("the node already hosts its maximum number of tenants")]
    Tenants,
    #[error("the node budget cannot fund the tenant's required memory")]
    Memory,
}

/// The node's admitted tenants and their budgets.
pub struct TenantAdmission {
    node: MemoryBudget,
    policy: AdmissionPolicy,
    admitted: BTreeMap<TenantId, FleetTenant>,
}
impl TenantAdmission {
    /// A node starts with its own tenant admitted on the budget the fleet
    /// was spawned with.
    pub fn new(
        node: MemoryBudget,
        policy: AdmissionPolicy,
        tenant: TenantId,
        budget: MemoryBudget,
    ) -> Self {
        let mut admitted = BTreeMap::new();
        admitted.insert(
            tenant,
            FleetTenant {
                tenant,
                weight: policy.weight,
                budget,
            },
        );
        Self {
            node,
            policy,
            admitted,
        }
    }
    pub fn policy(&self) -> AdmissionPolicy {
        self.policy
    }
    pub fn is_admitted(&self, tenant: TenantId) -> bool {
        self.admitted.contains_key(&tenant)
    }
    pub fn budget(&self, tenant: TenantId) -> Option<&MemoryBudget> {
        self.admitted.get(&tenant).map(|tenant| &tenant.budget)
    }
    pub fn tenants(&self) -> impl Iterator<Item = &FleetTenant> {
        self.admitted.values()
    }
    /// Admit one more tenant whose sessions declare `required_memory`, or
    /// say why the node cannot. An admitted tenant is answered again with
    /// the same allowance; nothing is retained on refusal.
    pub fn admit(
        &mut self,
        tenant: TenantId,
        required_memory: u64,
    ) -> Result<FleetTenant, AdmissionRefusal> {
        if let Some(existing) = self.admitted.get(&tenant) {
            return Ok(existing.clone());
        }
        if self.admitted.len() >= self.policy.max_tenants {
            return Err(AdmissionRefusal::Tenants);
        }
        let stats = self.node.stats();
        let free = u64::try_from(stats.limit.saturating_sub(stats.used)).unwrap_or(u64::MAX);
        if required_memory > free
            || required_memory > u64::try_from(self.policy.tenant_memory).unwrap_or(u64::MAX)
        {
            return Err(AdmissionRefusal::Memory);
        }
        let budget = self
            .node
            .child(
                self.policy.tenant_memory,
                self.policy.tenant_completion_reserve,
            )
            .map_err(|_| AdmissionRefusal::Memory)?;
        let admitted = FleetTenant {
            tenant,
            weight: self.policy.weight,
            budget,
        };
        self.admitted.insert(tenant, admitted.clone());
        Ok(admitted)
    }
    /// A bounded view of the node's admission state for diagnostics.
    pub fn report(
        &self,
        disk: &DiskStats,
        usage: &BTreeMap<TenantId, QueueUsage>,
    ) -> AdmissionReport {
        let node = self.node.stats();
        let mut tenants = Vec::new();
        if tenants.try_reserve_exact(self.admitted.len()).is_ok() {
            for (id, tenant) in &self.admitted {
                let stats = tenant.budget.stats();
                let queue = usage.get(id);
                tenants.push(TenantReport {
                    tenant: *id,
                    weight: tenant.weight,
                    memory_limit: to_u64(stats.limit),
                    memory_used: to_u64(stats.used),
                    sessions: queue.map_or(0, |usage| usage.sessions),
                    queued_items: queue.map_or(0, |usage| usage.queued),
                    queued_bytes: to_u64(queue.map_or(0, |usage| usage.bytes)),
                });
            }
        }
        AdmissionReport {
            max_tenants: self.policy.max_tenants,
            memory_limit: to_u64(node.limit),
            memory_used: to_u64(node.used),
            memory_completion_reserve: to_u64(node.completion_reserve),
            disk_free: disk.free,
            disk_outstanding: disk.outstanding,
            disk_headroom: disk.headroom,
            tenants,
        }
    }
}
fn to_u64(bytes: usize) -> u64 {
    u64::try_from(bytes).unwrap_or(u64::MAX)
}

/// What the node hosts and what it has left, as reported by the agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmissionReport {
    pub max_tenants: usize,
    pub memory_limit: u64,
    pub memory_used: u64,
    pub memory_completion_reserve: u64,
    /// The volume's free bytes at the last sample, once known.
    pub disk_free: Option<u64>,
    /// Bytes promised to queued durable writes.
    pub disk_outstanding: u64,
    pub disk_headroom: u64,
    pub tenants: Vec<TenantReport>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantReport {
    pub tenant: TenantId,
    pub weight: u32,
    pub memory_limit: u64,
    pub memory_used: u64,
    /// Sessions of the tenant with queued work.
    pub sessions: usize,
    pub queued_items: usize,
    pub queued_bytes: u64,
}

#[cfg(test)]
#[path = "admission_tests.rs"]
mod tests;
