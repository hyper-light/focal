//! Membership of the directory's partition groups (24 §13; the audit's
//! F24), journaled like the root's: one latest request per node, retained
//! until its receipt is known, so a lost reply is recovered by reference and
//! never re-issued as a fresh change.
use super::*;
use crate::network_admin::{PartitionAdminCommand, PartitionAdminReply};
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
        if actual != partition
            || group == [0; 16]
            || configuration.identity.cluster.0 != self.identity.cluster
            || configuration.identity.group != group
            || configuration.configuration_index > configuration.applied_index
            || record
                .as_ref()
                .is_some_and(|record| record.index != configuration.configuration_index)
        {
            return Err(ClusterAdminError::Invalid);
        }
        configuration
            .configuration
            .validate()
            .map_err(|_| ClusterAdminError::Invalid)?;
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
        // committed, and only a request it never took is still pending.
        if state
            .latest
            .as_ref()
            .is_some_and(|latest| latest.receipt.is_none() && !latest.superseded)
        {
            self.partition_drive(&mut journal, &mut state).await?;
        }
        let current = self.partition_configuration(partition).await?;
        if expected_index.is_some_and(|index| index != current.configuration.configuration_index) {
            return Err(ControlFailure::CompareFailed.into());
        }
        change
            .apply_to(&current.configuration.configuration)
            .map_err(|_| ClusterAdminError::Invalid)?;
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
