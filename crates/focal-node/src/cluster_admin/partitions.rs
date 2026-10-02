//! Membership of the directory's partition groups (24 §13; the audit's
//! F24), journaled like the root's: one latest request per node, retained
//! until its receipt is known, so a lost reply is recovered by reference and
//! never re-issued as a fresh change; a group's leadership handed on; and a
//! leaving node's seats vacated, which `cluster nodes remove` does before
//! the node leaves the root (its seats' permits end with its root
//! membership, and a group must not count a voter that is gone).
use super::*;
use crate::network_admin::{PartitionAdminCommand, PartitionAdminReply};
use focal_control::ControlTransfer;
use std::collections::BTreeMap;

const DIRECTORY: &str = "PARTITION.admin";
const MARKER: &str = "PARTITION.admin.initialized";
/// The partitions one node may administer: every one it may host, and the
/// first (`network_directory::MAX_HOSTED_PARTITIONS` + 1).
const MAX_PARTITIONS: usize = 33;

#[derive(Serialize, Deserialize)]
struct State {
    schema: u16,
    identity: NodeIdentity,
    next: u64,
    /// The next request sequence under the administrator's principal, per
    /// partition group: each group keeps its own retry window.
    sequences: BTreeMap<[u8; 16], u64>,
    latest: Option<Intent>,
}
#[derive(Serialize, Deserialize)]
struct Intent {
    operation: u64,
    partition: [u8; 16],
    group: [u8; 16],
    request: ControlRequest,
    receipt: Option<ControlReceipt>,
    superseded: bool,
}
/// A partition group's configuration as this node's replica applied it.
pub struct PartitionConfiguration {
    pub group: [u8; 16],
    pub leader: u64,
    pub configuration: ControlConfiguration,
    pub record: Option<ControlMembershipRecord>,
}
impl ClusterAdmin {
    /// The configuration of a partition's group, from this node's replica.
    pub async fn partition_configuration(
        &self,
        partition: [u8; 16],
    ) -> Result<PartitionConfiguration> {
        let PartitionAdminReply::Configuration {
            partition: actual,
            group,
            leader,
            configuration,
            record,
        } = self
            .partition_exchange(PartitionAdminCommand::Configuration { partition })
            .await?
        else {
            return Err(ClusterAdminError::Invalid);
        };
        if actual != partition || group == [0; 16] {
            return Err(ClusterAdminError::Inconsistent(
                "another partition or no group",
            ));
        }
        if configuration.identity.cluster.0 != self.identity.cluster
            || configuration.identity.group != group
        {
            return Err(ClusterAdminError::Inconsistent("the replica's identity"));
        }
        if configuration.configuration_index > configuration.applied_index {
            return Err(ClusterAdminError::Inconsistent(
                "configuration index ahead of the applied index",
            ));
        }
        if record
            .as_ref()
            .is_some_and(|record| record.index != configuration.configuration_index)
        {
            return Err(ClusterAdminError::Inconsistent(
                "membership record at another index than the configuration",
            ));
        }
        configuration
            .configuration
            .validate()
            .map_err(|_| ClusterAdminError::Inconsistent("the configuration itself"))?;
        Ok(PartitionConfiguration {
            group,
            leader,
            configuration: *configuration,
            record: record.map(|record| *record),
        })
    }
    pub async fn partition_show(&self, partition: [u8; 16]) -> Result<AdminResult> {
        let current = self.partition_configuration(partition).await?;
        Ok(AdminResult::Configuration {
            configuration: configuration_view(current.configuration),
        })
    }
    /// One membership change of a partition's group, fenced on the
    /// configuration index it was decided against.
    pub async fn partition_change(
        &self,
        partition: [u8; 16],
        change: MembershipChange,
        expected_index: Option<u64>,
    ) -> Result<AdminResult> {
        let (mut journal, mut state) = self.partition_journal(true)?;
        // A request whose outcome is still unknown is asked again under its
        // exact identity first: the group's retry window answers what it
        // committed, and only a request it never took is still pending. An
        // operator asking for that same change again is given its receipt —
        // a change that took is not asked for twice (the second would not
        // apply: a learner the lost reply promoted is a voter now).
        if let Some(latest) = state.latest.as_ref()
            && latest.receipt.is_none()
            && !latest.superseded
        {
            let same = latest.partition == partition
                && matches!(
                    &latest.request.command,
                    ControlCommand::Membership(command) if command.change == change
                );
            let driven = self.partition_drive(&mut journal, &mut state).await?;
            if same {
                return Ok(driven);
            }
        }
        let current = self.partition_configuration(partition).await?;
        if expected_index.is_some_and(|index| index != current.configuration.configuration_index) {
            return Err(ControlFailure::CompareFailed.into());
        }
        change
            .apply_to(&current.configuration.configuration)
            .map_err(|error| match error {
                focal_consensus::ConsensusError::Configuration(reason) => {
                    ClusterAdminError::Inapplicable {
                        reason,
                        voters: current.configuration.configuration.voters.clone(),
                        learners: current.configuration.configuration.learners.clone(),
                        configuration_index: current.configuration.configuration_index,
                        applied_index: current.configuration.applied_index,
                    }
                }
                _ => ClusterAdminError::Invalid,
            })?;
        let operation = state.next;
        let next = operation
            .checked_add(1)
            .ok_or(ClusterAdminError::Capacity)?;
        if !state.sequences.contains_key(&current.group) {
            if state.sequences.len() >= MAX_PARTITIONS {
                return Err(ClusterAdminError::Capacity);
            }
            state.sequences.insert(current.group, 1);
        }
        let sequence = *state
            .sequences
            .get(&current.group)
            .ok_or(ClusterAdminError::Corrupt)?;
        sequence.checked_add(1).ok_or(ClusterAdminError::Capacity)?;
        let request = ControlRequest {
            id: ControlRequestId {
                client: admin_principal(&self.identity).0,
                sequence,
            },
            acknowledged_through: sequence.checked_sub(1).ok_or(ClusterAdminError::Corrupt)?,
            command: ControlCommand::Membership(ControlMembershipCommand {
                expected_configuration_index: current.configuration.configuration_index,
                expected: current.configuration.configuration,
                change,
            }),
        };
        state.next = next;
        state.latest = Some(Intent {
            operation,
            partition,
            group: current.group,
            request,
            receipt: None,
            superseded: false,
        });
        save_value(&mut journal, &state)?;
        self.partition_drive(&mut journal, &mut state).await
    }
    /// Hand the partition group's leadership to `target`, fenced on the
    /// configuration index when given; initiation, as the root's transfer.
    pub async fn partition_transfer(
        &self,
        partition: [u8; 16],
        target: u64,
        expected_index: Option<u64>,
    ) -> Result<AdminResult> {
        let current = self.partition_configuration(partition).await?;
        if expected_index.is_some_and(|index| index != current.configuration.configuration_index) {
            return Err(ControlFailure::CompareFailed.into());
        }
        if target == 0 || !current.configuration.configuration.voters.contains(&target) {
            return Err(ClusterAdminError::Invalid);
        }
        let request = ControlTransfer {
            expected_configuration_index: current.configuration.configuration_index,
            expected: current.configuration.configuration,
            target,
        };
        match self
            .partition_exchange(PartitionAdminCommand::Transfer {
                partition,
                request: Box::new(request),
            })
            .await?
        {
            PartitionAdminReply::TransferInitiated {
                partition: actual,
                target: initiated,
                ..
            } if actual == partition && initiated == target => {
                Ok(AdminResult::TransferInitiated { target })
            }
            _ => Err(ClusterAdminError::Invalid),
        }
    }
    /// The partition groups whose seats name `node`, by the root's grants.
    pub async fn partitions_seating(&self, node: u64) -> Result<Vec<[u8; 16]>> {
        let placement = self.placement_view().await?;
        let mut seating = Vec::new();
        for group in placement
            .control
            .iter()
            .flat_map(|control| control.partitions.iter())
        {
            if !group.voters.contains(&node) && !group.learners.contains(&node) {
                continue;
            }
            let Some(partition) = group.partition.as_deref() else {
                continue;
            };
            let id =
                focal_client::input::parse_id(partition).map_err(|_| ClusterAdminError::Invalid)?;
            if seating.len() >= MAX_PARTITIONS {
                return Err(ClusterAdminError::Capacity);
            }
            seating
                .try_reserve_exact(1)
                .map_err(|_| ClusterAdminError::Capacity)?;
            seating.push(id);
        }
        Ok(seating)
    }
    /// Take `node` out of a partition group it is seated in: when it leads
    /// the group its leadership is handed to another voter first, then its
    /// removal is one exact journaled request, asked again — bounded, as the
    /// root's removal is — while the group is not ready, mid-change or the
    /// outcome unknown. Returns whether the configuration changed.
    pub async fn partition_vacate(&self, partition: [u8; 16], node: u64) -> Result<bool> {
        let mut changed = false;
        for _ in 0..=REMOVE_DRAIN_POLLS {
            let current = self.partition_configuration(partition).await?;
            let configuration = &current.configuration.configuration;
            if !configuration.contains(node) {
                return Ok(changed);
            }
            if configuration.voters_outgoing.is_empty() {
                if current.leader == node && configuration.voters.contains(&node) {
                    self.partition_lead_elsewhere(partition, node).await?;
                    continue;
                }
                match self
                    .partition_change(
                        partition,
                        MembershipChange::Remove { node },
                        Some(current.configuration.configuration_index),
                    )
                    .await
                {
                    Ok(_) => {
                        changed = true;
                        continue;
                    }
                    Err(
                        ClusterAdminError::Pending
                        | ClusterAdminError::Control(
                            ControlFailure::NotReady
                            | ControlFailure::CompareFailed
                            | ControlFailure::Unavailable
                            | ControlFailure::OutcomeUnknown,
                        ),
                    ) => {}
                    Err(error) => return Err(error),
                }
            }
            tokio::time::sleep(REMOVE_DRAIN_POLL).await;
        }
        Err(ClusterAdminError::PartitionPending {
            node,
            partition: hex(&partition),
        })
    }
    /// Hand a partition group's leadership away from `leaving`: to this
    /// node when it votes in the group (it may ask to lead itself through
    /// the leader), else to another voter; bounded, as the root's is.
    async fn partition_lead_elsewhere(&self, partition: [u8; 16], leaving: u64) -> Result<()> {
        for _ in 0..=REMOVE_DRAIN_POLLS {
            let current = self.partition_configuration(partition).await?;
            if current.leader != 0 && current.leader != leaving {
                return Ok(());
            }
            let voters = &current.configuration.configuration.voters;
            let target = if voters.contains(&self.identity.node) {
                Some(self.identity.node)
            } else {
                voters.iter().copied().find(|voter| *voter != leaving)
            };
            if current.leader == leaving
                && let Some(target) = target
            {
                // Refused while an earlier transfer or an election is in
                // progress; the next read says where the group leads.
                match self
                    .partition_transfer(
                        partition,
                        target,
                        Some(current.configuration.configuration_index),
                    )
                    .await
                {
                    Ok(_) | Err(ClusterAdminError::Control(_)) => {}
                    Err(error) => return Err(error),
                }
            }
            tokio::time::sleep(REMOVE_DRAIN_POLL).await;
        }
        Err(ClusterAdminError::LeaderLeaving(leaving))
    }
    pub async fn partition_retry(&self, reference: &str) -> Result<AdminResult> {
        let (mut journal, mut state) = self.partition_journal(false)?;
        let latest = state.latest.as_ref().ok_or(ClusterAdminError::Expired)?;
        if reference != partition_reference(self.identity.node, latest.operation)
            || latest.superseded
        {
            return Err(ClusterAdminError::Expired);
        }
        self.partition_drive(&mut journal, &mut state).await
    }
    /// A request whose outcome was lost is asked again under its exact
    /// identity: the group's retry window answers what it committed, and a
    /// configuration that moved past the request's fence supersedes it.
    pub async fn partition_reconcile(&self, reference: &str) -> Result<AdminResult> {
        let (mut journal, mut state) = self.partition_journal(false)?;
        let latest = state.latest.as_ref().ok_or(ClusterAdminError::Expired)?;
        if reference != partition_reference(self.identity.node, latest.operation) {
            return Err(ClusterAdminError::Expired);
        }
        if latest.receipt.is_some() || latest.superseded {
            return state_view(self.identity.node, &state);
        }
        match self.partition_drive(&mut journal, &mut state).await {
            Ok(result) => Ok(result),
            Err(ClusterAdminError::Control(
                ControlFailure::CompareFailed | ControlFailure::RetryExpired,
            )) => {
                let current = self
                    .partition_configuration(latest_partition(&state)?)
                    .await?;
                let fence = match &state
                    .latest
                    .as_ref()
                    .ok_or(ClusterAdminError::Corrupt)?
                    .request
                    .command
                {
                    ControlCommand::Membership(command) => command.expected_configuration_index,
                    _ => return Err(ClusterAdminError::Corrupt),
                };
                if current.configuration.configuration_index > fence {
                    state
                        .latest
                        .as_mut()
                        .ok_or(ClusterAdminError::Corrupt)?
                        .superseded = true;
                    save_value(&mut journal, &state)?;
                }
                state_view(self.identity.node, &state)
            }
            Err(error) => Err(error),
        }
    }
    async fn partition_drive(
        &self,
        journal: &mut PrivateJournal,
        state: &mut State,
    ) -> Result<AdminResult> {
        let latest = state.latest.as_ref().ok_or(ClusterAdminError::Corrupt)?;
        if let Some(receipt) = latest.receipt {
            return Ok(receipt_view_named(
                partition_reference(self.identity.node, latest.operation),
                receipt,
            ));
        }
        if latest.superseded {
            return Err(ClusterAdminError::Expired);
        }
        let operation = latest.operation;
        let request = latest.request.clone();
        let PartitionAdminReply::Committed {
            partition,
            group,
            receipt,
        } = self
            .partition_exchange(PartitionAdminCommand::Change {
                partition: latest.partition,
                request: Box::new(request.clone()),
            })
            .await?
        else {
            return Err(ClusterAdminError::Invalid);
        };
        if partition != latest.partition || group != latest.group {
            return Err(ClusterAdminError::Invalid);
        }
        validate_receipt(&request, &receipt)?;
        let latest = state.latest.as_mut().ok_or(ClusterAdminError::Corrupt)?;
        latest.receipt = Some(receipt);
        let next_sequence = receipt
            .request
            .sequence
            .checked_add(1)
            .ok_or(ClusterAdminError::Capacity)?;
        state.sequences.insert(group, next_sequence);
        save_value(journal, state)?;
        Ok(receipt_view_named(
            partition_reference(self.identity.node, operation),
            receipt,
        ))
    }
    async fn partition_exchange(
        &self,
        command: PartitionAdminCommand,
    ) -> Result<PartitionAdminReply> {
        let bytes = self
            .exchange_bytes(AdminCommand::Partition(Box::new(command)))
            .await?;
        let (reply, tail): (PartitionAdminReply, _) =
            postcard::take_from_bytes(&bytes).map_err(|_| ClusterAdminError::Invalid)?;
        if !tail.is_empty() {
            return Err(ClusterAdminError::Invalid);
        }
        match reply {
            PartitionAdminReply::Rejected(error) => Err(error.into()),
            reply => Ok(reply),
        }
    }
    fn partition_journal(&self, create: bool) -> Result<(PrivateJournal, State)> {
        let fresh = initialize_named(&self.root, &self.identity, create, DIRECTORY, MARKER)?;
        let mut journal = PrivateJournal::open(self.root.join(DIRECTORY))?;
        let state = match journal.read()? {
            Some(bytes) => {
                let (state, tail): (State, _) =
                    postcard::take_from_bytes(&bytes).map_err(|_| ClusterAdminError::Corrupt)?;
                if !tail.is_empty() {
                    return Err(ClusterAdminError::Corrupt);
                }
                state
            }
            None if fresh => {
                let state = State {
                    schema: 1,
                    identity: self.identity.clone(),
                    next: 1,
                    sequences: BTreeMap::new(),
                    latest: None,
                };
                save_value(&mut journal, &state)?;
                state
            }
            None => return Err(ClusterAdminError::Corrupt),
        };
        if state.schema != 1
            || state.identity != self.identity
            || state.next == 0
            || state.sequences.len() > MAX_PARTITIONS
            || state.sequences.values().any(|sequence| *sequence == 0)
        {
            return Err(ClusterAdminError::Corrupt);
        }
        if let Some(latest) = &state.latest {
            if latest.request.id.client != admin_principal(&self.identity).0
                || latest.operation.checked_add(1) != Some(state.next)
                || latest.partition == [0; 16]
                || latest.group == [0; 16]
                || latest.request.acknowledged_through.checked_add(1)
                    != Some(latest.request.id.sequence)
                || (latest.receipt.is_some() && latest.superseded)
            {
                return Err(ClusterAdminError::Corrupt);
            }
            if let Some(receipt) = &latest.receipt {
                validate_receipt(&latest.request, receipt)?;
            }
        } else if state.next != 1 {
            return Err(ClusterAdminError::Corrupt);
        }
        Ok((journal, state))
    }
}
fn latest_partition(state: &State) -> Result<[u8; 16]> {
    Ok(state
        .latest
        .as_ref()
        .ok_or(ClusterAdminError::Corrupt)?
        .partition)
}
fn state_view(node: u64, state: &State) -> Result<AdminResult> {
    let latest = state.latest.as_ref().ok_or(ClusterAdminError::Expired)?;
    match latest.receipt {
        Some(receipt) => Ok(receipt_view_named(
            partition_reference(node, latest.operation),
            receipt,
        )),
        None => Ok(AdminResult::Request {
            operation_id: partition_reference(node, latest.operation),
            state: if latest.superseded {
                "Superseded"
            } else {
                "Pending"
            }
            .into(),
        }),
    }
}
fn receipt_view_named(operation_id: String, receipt: ControlReceipt) -> AdminResult {
    AdminResult::Committed {
        operation_id,
        client: hex(&receipt.request.client),
        sequence: receipt.request.sequence,
        request_hash: hex(&receipt.request_hash),
        committed_index: receipt.committed_index,
        committed_term: receipt.committed_term,
    }
}
/// `p1:<node>:<operation>`: a partition-group request of this node's
/// administrator, distinct from the root's `a1:` references.
pub fn partition_reference(node: u64, operation: u64) -> String {
    format!("p1:{node:016x}:{operation:016x}")
}
