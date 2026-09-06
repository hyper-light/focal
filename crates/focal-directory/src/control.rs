use crate::*;
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use focal_model::LedgerId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegionRecord {
    pub id: RegionId,
    pub label: String,
    pub authority_epoch: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Delegation {
    pub namespace: NamespaceRange,
    pub partition: PartitionId,
    pub region: RegionId,
    pub log_group: LogGroupId,
    pub epoch: u64,
    pub activation: Option<DelegationFence>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RootCheckpoint {
    pub schema: u16,
    pub cluster: ClusterId,
    pub revision: u64,
    pub regions: BTreeMap<RegionId, RegionRecord>,
    pub delegations: BTreeMap<NamespaceKey, Delegation>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RootCommand {
    pub expected_revision: u64,
    pub operation: RootOperation,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(
    clippy::large_enum_variant,
    reason = "bounded owned command; inline fields avoid extra per-command allocations"
)]
pub enum RootOperation {
    RegisterRegion {
        region: RegionRecord,
        expected_epoch: Option<u64>,
    },
    Delegate {
        delegation: Delegation,
    },
    Transfer {
        start: NamespaceKey,
        expected_epoch: u64,
        destination: Delegation,
        fence: DelegationFence,
    },
}
#[derive(Debug, Clone, Copy)]
pub struct RootConfig {
    pub max_regions: usize,
    pub max_delegations: usize,
    pub max_label_bytes: usize,
}
impl Default for RootConfig {
    fn default() -> Self {
        Self {
            max_regions: 256,
            max_delegations: 4096,
            max_label_bytes: 128,
        }
    }
}
struct RootVersion {
    state: RootCheckpoint,
    _allocation: Allocation,
}
pub struct RootDirectory {
    root: RootVersion,
    owner: focal_memory::OwnerId,
    config: RootConfig,
    budget: MemoryBudget,
}
pub struct PreparedRootUpdate {
    owner: focal_memory::OwnerId,
    base_revision: u64,
    next: RootVersion,
}
impl PreparedRootUpdate {
    pub fn checkpoint(&self) -> &RootCheckpoint {
        &self.next.state
    }
}

impl RootDirectory {
    pub fn new(
        cluster: ClusterId,
        config: RootConfig,
        budget: MemoryBudget,
    ) -> Result<Self, DirectoryError> {
        Self::restore(
            RootCheckpoint {
                schema: 1,
                cluster,
                revision: 0,
                regions: BTreeMap::new(),
                delegations: BTreeMap::new(),
            },
            config,
            budget,
        )
    }
    pub fn restore(
        state: RootCheckpoint,
        config: RootConfig,
        budget: MemoryBudget,
    ) -> Result<Self, DirectoryError> {
        validate_root(&state, config)?;
        let allocation = budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                root_charge(&state)?,
            )?
            .commit();
        Ok(Self {
            owner: focal_memory::OwnerId::new()?,
            root: RootVersion {
                state,
                _allocation: allocation,
            },
            config,
            budget,
        })
    }
    pub fn checkpoint(&self) -> &RootCheckpoint {
        &self.root.state
    }
    pub fn revision(&self) -> u64 {
        self.root.state.revision
    }
    /// One interval lookup, never a global session/claim table.
    pub fn resolve(&self, ledger: LedgerId) -> Option<&Delegation> {
        self.root
            .state
            .delegations
            .range(..=NamespaceKey::of(ledger))
            .next_back()
            .map(|(_, value)| value)
            .filter(|value| value.namespace.contains(ledger))
    }
    pub fn prepare(
        &self,
        command: &RootCommand,
        authority: &impl AuthorityVerifier,
    ) -> Result<PreparedRootUpdate, DirectoryError> {
        if command.expected_revision != self.revision() {
            return Err(DirectoryError::CompareFailed);
        }
        if let RootOperation::RegisterRegion { region, .. } = &command.operation
            && (region.label.is_empty() || region.label.len() > self.config.max_label_bytes)
        {
            return Err(DirectoryError::Invalid("region label"));
        }
        let revision = self
            .revision()
            .checked_add(1)
            .ok_or(DirectoryError::CounterExhausted)?;
        let extra = match &command.operation {
            RootOperation::RegisterRegion { region, .. } => add(
                tree_row::<(RegionId, RegionRecord)>(),
                region.label.capacity(),
            )?,
            _ => mul(tree_row::<(NamespaceKey, Delegation)>(), 2)?,
        };
        let allocation = self
            .budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                add(root_charge(&self.root.state)?, extra)?,
            )?
            .commit();
        let mut state = self.root.state.clone();
        match &command.operation {
            RootOperation::RegisterRegion {
                region,
                expected_epoch,
            } => {
                let previous = state.regions.get(&region.id).map(|old| old.authority_epoch);
                if previous != *expected_epoch {
                    return Err(DirectoryError::CompareFailed);
                }
                if region.authority_epoch
                    != previous
                        .unwrap_or(0)
                        .checked_add(1)
                        .ok_or(DirectoryError::CounterExhausted)?
                {
                    return Err(DirectoryError::StaleEpoch);
                }
                if state
                    .regions
                    .values()
                    .any(|old| old.id != region.id && old.label == region.label)
                {
                    return Err(DirectoryError::Duplicate);
                }
                state.regions.insert(region.id, region.clone());
            }
            RootOperation::Delegate { delegation } => {
                if delegation.epoch != 1 || delegation.activation.is_some() {
                    return Err(DirectoryError::StaleEpoch);
                }
                if state.delegations.values().any(|old| {
                    old.namespace.overlaps(delegation.namespace)
                        || old.partition == delegation.partition
                        || old.log_group == delegation.log_group
                }) {
                    return Err(DirectoryError::Duplicate);
                }
                state
                    .delegations
                    .insert(delegation.namespace.start, delegation.clone());
            }
            RootOperation::Transfer {
                start,
                expected_epoch,
                destination,
                fence,
            } => {
                let old = state
                    .delegations
                    .get(start)
                    .ok_or(DirectoryError::Missing)?;
                if old.epoch != *expected_epoch {
                    return Err(DirectoryError::CompareFailed);
                }
                let next_epoch = expected_epoch
                    .checked_add(1)
                    .ok_or(DirectoryError::CounterExhausted)?;
                if fence.cluster != state.cluster {
                    return Err(DirectoryError::WrongCluster);
                }
                if fence.source != old.partition
                    || fence.destination != destination.partition
                    || fence.namespace != old.namespace
                    || destination.namespace != old.namespace
                    || destination.epoch != next_epoch
                    || fence.from_epoch != *expected_epoch
                    || fence.to_epoch != next_epoch
                    || fence.sealed_revision == 0
                    || !types::nonzero_hash(fence.checkpoint)
                    || !types::nonzero_hash(fence.destination_ready)
                {
                    return Err(DirectoryError::StaleEpoch);
                }
                if old.partition == destination.partition
                    || state.delegations.values().any(|value| {
                        value.namespace.start != *start
                            && (value.partition == destination.partition
                                || value.log_group == destination.log_group)
                    })
                {
                    return Err(DirectoryError::Duplicate);
                }
                authority.verify_delegation(fence)?;
                let mut destination = destination.clone();
                destination.activation = Some(fence.clone());
                state.delegations.insert(*start, destination);
            }
        }
        state.revision = revision;
        validate_root(&state, self.config)?;
        Ok(PreparedRootUpdate {
            owner: self.owner,
            base_revision: self.revision(),
            next: RootVersion {
                state,
                _allocation: allocation,
            },
        })
    }
    /// Publish after the authoritative metadata log commits the exact command.
    pub fn publish(&mut self, update: PreparedRootUpdate) -> Result<(), DirectoryError> {
        if self.owner != update.owner || self.revision() != update.base_revision {
            return Err(DirectoryError::StalePreparation);
        }
        self.root = update.next;
        Ok(())
    }
}
fn validate_root(state: &RootCheckpoint, config: RootConfig) -> Result<(), DirectoryError> {
    if state.schema != 1 || config.max_regions == 0 || config.max_delegations == 0 {
        return Err(DirectoryError::Invalid("root schema or capacity"));
    }
    if state.regions.len() > config.max_regions || state.delegations.len() > config.max_delegations
    {
        return Err(DirectoryError::Capacity);
    }
    for (id, region) in &state.regions {
        if *id != region.id
            || region.authority_epoch == 0
            || region.label.is_empty()
            || region.label.len() > config.max_label_bytes
        {
            return Err(DirectoryError::Invalid("region metadata"));
        }
        if state
            .regions
            .range(..id)
            .any(|(_, previous)| previous.label == region.label)
        {
            return Err(DirectoryError::Duplicate);
        }
    }
    let mut previous: Option<&Delegation> = None;
    for (start, delegation) in &state.delegations {
        delegation.namespace.validate()?;
        if *start != delegation.namespace.start
            || delegation.epoch == 0
            || !state.regions.contains_key(&delegation.region)
        {
            return Err(DirectoryError::Invalid("delegation metadata"));
        }
        if previous.is_some_and(|old| old.namespace.overlaps(delegation.namespace)) {
            return Err(DirectoryError::Duplicate);
        }
        if state.delegations.range(..start).any(|(_, previous)| {
            previous.partition == delegation.partition || previous.log_group == delegation.log_group
        }) {
            return Err(DirectoryError::Duplicate);
        }
        validate_delegation(delegation, state.cluster)?;
        previous = Some(delegation);
    }
    Ok(())
}
pub(crate) fn validate_delegation(
    delegation: &Delegation,
    cluster: ClusterId,
) -> Result<(), DirectoryError> {
    match &delegation.activation {
        None if delegation.epoch != 1 => return Err(DirectoryError::StaleEpoch),
        Some(fence) => {
            if fence.cluster != cluster
                || fence.destination != delegation.partition
                || fence.source == fence.destination
                || fence.namespace != delegation.namespace
                || fence.to_epoch != delegation.epoch
                || fence.from_epoch.checked_add(1) != Some(fence.to_epoch)
                || fence.sealed_revision == 0
                || !types::nonzero_hash(fence.checkpoint)
                || !types::nonzero_hash(fence.destination_ready)
            {
                return Err(DirectoryError::StaleEpoch);
            }
        }
        None => {}
    }
    Ok(())
}
fn root_charge(state: &RootCheckpoint) -> Result<usize, DirectoryError> {
    let mut bytes = add(
        add(size_of::<RootVersion>(), ALLOCATOR_OVERHEAD)?,
        mul(state.regions.len(), tree_row::<(RegionId, RegionRecord)>())?,
    )?;
    bytes = add(
        bytes,
        mul(
            state.delegations.len(),
            tree_row::<(NamespaceKey, Delegation)>(),
        )?,
    )?;
    for region in state.regions.values() {
        bytes = add(bytes, region.label.capacity())?;
    }
    Ok(bytes)
}
