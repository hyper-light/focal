//! Namespace split and merge, executed by the placement agent on the node
//! that leads both the root and the partition it reshapes
//! ([24](../../../docs/archictecutre/24-placement-execution-and-fleet-control.md) §13).
//! Every step is a committed fact or a journaled intent: a sealed source, a
//! hosted destination on the sealed image, one root fence signed by both
//! groups, the destination's install, the source's release; a merge seals
//! the upper partition, commits the root merge and absorbs it below.
use super::*;
use crate::{
    control_host::RootObservation,
    directory_bootstrap::{PartitionPlan, next_first_directory_command},
    network_service::HostRequest,
};
use focal_control::{ControlEvidence, VerifiedRootCommand};
use focal_directory::{
    ClusterId, DelegationFence, LogGroupId, NamespaceKey, RootCheckpoint, RootCommand,
    RootOperation, partition_checkpoint_digest, split_group_id, split_image, split_partition_id,
};

/// Sessions at which a partition splits (half the partition capacity).
pub const DEFAULT_SPLIT_SESSIONS: usize = 2048;
/// Sessions under which two adjacent partitions merge (both must be under it).
pub const DEFAULT_MERGE_SESSIONS: usize = 256;
const SPLIT_ENV: &str = "FOCAL_PARTITION_SPLIT_SESSIONS";
const MERGE_ENV: &str = "FOCAL_PARTITION_MERGE_SESSIONS";

#[cfg(any(test, feature = "test-support"))]
static SPLIT_OVERRIDE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
#[cfg(any(test, feature = "test-support"))]
static MERGE_OVERRIDE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
/// In-process test knob: `split` sessions split a partition, `merge` is the
/// bound both halves must be under to merge (`usize::MAX` for "never").
#[cfg(any(test, feature = "test-support"))]
pub fn override_thresholds(split: usize, merge: usize) {
    SPLIT_OVERRIDE.store(split, std::sync::atomic::Ordering::Release);
    MERGE_OVERRIDE.store(
        merge.saturating_add(1),
        std::sync::atomic::Ordering::Release,
    );
}
/// The thresholds in force: the defaults, or the knobs when set.
pub fn thresholds() -> (usize, usize) {
    #[cfg(any(test, feature = "test-support"))]
    {
        let split = SPLIT_OVERRIDE.load(std::sync::atomic::Ordering::Acquire);
        let merge = MERGE_OVERRIDE.load(std::sync::atomic::Ordering::Acquire);
        if split > 0 && merge > 0 {
            return (split, merge.saturating_sub(1));
        }
    }
    let read = |name: &str, default: usize| {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value >= 1)
            .unwrap_or(default)
    };
    let split = read(SPLIT_ENV, DEFAULT_SPLIT_SESSIONS);
    let merge = read(MERGE_ENV, DEFAULT_MERGE_SESSIONS);
    (split, merge)
}
/// The operation identity of a reshape started from one partition state.
fn reshape_operation(cluster: [u8; 16], partition: PartitionId, revision: u64) -> OperationId {
    let mut hash = blake3::Hasher::new_derive_key("focal.directory.reshape-operation.v1");
    hash.update(&cluster);
    hash.update(&partition.0);
    hash.update(&revision.to_be_bytes());
    let mut id = [0; 16];
    for (target, source) in id.iter_mut().zip(hash.finalize().as_bytes()) {
        *target = *source;
    }
    OperationId(id)
}
fn evidence_from(proofs: Vec<focal_directory::AuthorityProof>, now: i64) -> ControlEvidence {
    let (authority_revision, enrollment_revision) = proofs.first().map_or((0, 0), |proof| {
        (
            proof.statement.authority_revision,
            proof.statement.enrollment_revision,
        )
    });
    ControlEvidence {
        authority_revision,
        enrollment_revision,
        decided_at: now,
        proofs,
    }
}

#[cfg(test)]
#[path = "partition_split_tests.rs"]
mod tests;

impl PlacementAgent {
    /// One reshape step for `directory`, the partition `delegation` names,
    /// which this node leads together with the root. Returns the step taken.
    #[allow(
        clippy::too_many_arguments,
        reason = "one bounded pass over borrowed observations; no state is retained"
    )]
    pub(super) async fn reshape(
        &mut self,
        handles: &NetworkHandles,
        pool: &PeerConnectionPool,
        observation: &RootObservation,
        root: &RootCheckpoint,
        delegation: &Delegation,
        host: &ControlHost,
        directory: &PartitionCheckpoint,
        observed: &[Observed],
        snapshot: &ControlSnapshot,
        installed: &ControlAuthoritySnapshot,
        now: i64,
    ) -> Result<Option<AgentStep>, AgentError> {
        let _ = host;
        let node = self.state.node;
        let cluster = self.state.genesis.founder.cluster;
        let (split_at, merge_at) = thresholds();
        let own = &directory.delegation;
        let _ = std::io::Write::write_fmt(
            &mut std::io::stderr().lock(),
            format_args!(
                "TRACE {}\n",
                format!(
                    "reshape partition {:?} sealed={} sessions={} split_at={} merge_at={} root_epoch={} own_epoch={}",
                    own.partition.0[..4].to_vec(),
                    directory.sealed.is_some(),
                    directory.sessions.len(),
                    split_at,
                    merge_at,
                    delegation.epoch,
                    own.epoch
                )
            ),
        );
        match &directory.sealed {
            None => {
                // A merge the root already committed: this partition still
                // holds its old namespace while the root names the union.
                if delegation.epoch == own.epoch.saturating_add(1)
                    && delegation.namespace != own.namespace
                    && delegation.activation.is_some_and(|fence| {
                        fence.destination == own.partition
                            && own.namespace.end == Some(fence.namespace.start)
                    })
                {
                    return self
                        .absorb(
                            handles, pool, delegation, directory, snapshot, installed, now,
                        )
                        .await
                        .map(Some);
                }
                if delegation.epoch != own.epoch || delegation.namespace != own.namespace {
                    // The root moved ahead of this partition in another way;
                    // the partition's own release or install catches up.
                    return Ok(None);
                }
                if directory.sessions.len() >= split_at {
                    let at = directory
                        .sessions
                        .keys()
                        .nth(directory.sessions.len().saturating_div(2))
                        .map(|ledger| NamespaceKey::of(*ledger))
                        .filter(|at| *at != own.namespace.start)
                        .ok_or(AgentError::Identity)?;
                    let operation = reshape_operation(cluster, own.partition, directory.revision);
                    let destination = split_partition_id(ClusterId(cluster), operation);
                    let command = ControlCommand::VerifiedPartition(VerifiedPartitionCommand {
                        command: PartitionCommand {
                            expected_revision: directory.revision,
                            delegation_epoch: own.epoch,
                            operation: PartitionOperation::SealForSplit {
                                operation,
                                destination,
                                next_epoch: own.epoch.checked_add(1).ok_or(AgentError::Capacity)?,
                                at,
                            },
                        },
                        evidence: control_evidence(installed, cluster, now, &self.budget)?,
                    });
                    if snapshot.revisions.partition != directory.revision {
                        return Err(AgentError::Identity);
                    }
                    return self.intend_partition(handles, command).await.map(Some);
                }
                // Merge the partition right above this one when both are small
                // and their union stays well under the split threshold.
                let Some(end) = own.namespace.end else {
                    return Ok(None);
                };
                let Some(right) = root.delegations.get(&end) else {
                    return Ok(None);
                };
                let Some((right_state, right_snapshot, right_installed)) =
                    observed_partition(observed, right.partition)
                else {
                    return Ok(None);
                };
                match &right_state.sealed {
                    None => {
                        let bound = merge_at.min(self.partition_config().max_absorb_sessions);
                        if right_state.delegation != *right
                            || directory.sessions.len() > merge_at
                            || right_state.sessions.len() > bound
                            || directory
                                .sessions
                                .len()
                                .saturating_add(right_state.sessions.len())
                                >= split_at
                        {
                            return Ok(None);
                        }
                        let operation =
                            reshape_operation(cluster, right.partition, right_state.revision);
                        let command = ControlCommand::VerifiedPartition(VerifiedPartitionCommand {
                            command: PartitionCommand {
                                expected_revision: right_state.revision,
                                delegation_epoch: right.epoch,
                                operation: PartitionOperation::SealForTransfer {
                                    operation,
                                    destination: own.partition,
                                    next_epoch: right
                                        .epoch
                                        .checked_add(1)
                                        .ok_or(AgentError::Capacity)?,
                                },
                            },
                            evidence: control_evidence(
                                right_installed,
                                cluster,
                                now,
                                &self.budget,
                            )?,
                        });
                        if right_snapshot.revisions.partition != right_state.revision {
                            return Err(AgentError::Identity);
                        }
                        self.current_partition = Some(right.partition);
                        let step = self.intend_partition(handles, command).await;
                        self.current_partition = Some(own.partition);
                        step.map(Some)
                    }
                    Some(seal)
                        if seal.destination == own.partition
                            && seal.moved == right_state.delegation.namespace =>
                    {
                        // The upper partition is sealed for this one: commit
                        // the root merge under both groups' signatures.
                        let digest = partition_checkpoint_digest(right_state)
                            .map_err(|_| AgentError::Identity)?;
                        let fence = DelegationFence {
                            cluster: ClusterId(cluster),
                            operation: seal.operation,
                            source: right.partition,
                            destination: own.partition,
                            namespace: right.namespace,
                            from_epoch: own.epoch,
                            to_epoch: own.epoch.checked_add(1).ok_or(AgentError::Capacity)?,
                            sealed_revision: seal.revision,
                            checkpoint: digest,
                            destination_ready: digest,
                        };
                        let proofs = self
                            .delegation_proofs(handles, right.log_group, own.log_group, fence, now)
                            .await?;
                        let command = ControlCommand::VerifiedRoot(VerifiedRootCommand {
                            command: RootCommand {
                                expected_revision: root.revision,
                                operation: RootOperation::Merge {
                                    start: own.namespace.start,
                                    right: right.namespace.start,
                                    expected_epoch: own.epoch,
                                    right_expected_epoch: right.epoch,
                                    fence,
                                },
                            },
                            evidence: evidence_from(proofs, now),
                        });
                        let journals = self.journals.as_mut().ok_or(AgentError::Identity)?;
                        journals.root.intend(&handles.control, command).await?;
                        let _ = node;
                        Ok(Some(AgentStep::Advanced))
                    }
                    Some(_) => Ok(None),
                }
            }
            Some(seal) if seal.moved != own.namespace => {
                self.continue_split(
                    handles,
                    pool,
                    observation,
                    root,
                    directory,
                    seal,
                    observed,
                    now,
                )
                .await
            }
            // Sealed whole: a merge or transfer source waits for its
            // destination and retires with it.
            Some(_) => Ok(None),
        }
    }
    /// The steps after a source sealed the upper part of its namespace.
    #[allow(
        clippy::too_many_arguments,
        reason = "one bounded pass over borrowed observations; no state is retained"
    )]
    async fn continue_split(
        &mut self,
        handles: &NetworkHandles,
        pool: &PeerConnectionPool,
        observation: &RootObservation,
        root: &RootCheckpoint,
        directory: &PartitionCheckpoint,
        seal: &focal_directory::PartitionSeal,
        observed: &[Observed],
        now: i64,
    ) -> Result<Option<AgentStep>, AgentError> {
        let _ = pool;
        let node = self.state.node;
        let cluster = self.state.genesis.founder.cluster;
        let own = &directory.delegation;
        let image = split_image(directory).map_err(|_| AgentError::Identity)?;
        let digest = partition_checkpoint_digest(&image).map_err(|_| AgentError::Identity)?;
        let fence = DelegationFence {
            cluster: ClusterId(cluster),
            operation: seal.operation,
            source: own.partition,
            destination: seal.destination,
            namespace: seal.moved,
            from_epoch: own.epoch,
            to_epoch: seal.next_epoch,
            sealed_revision: seal.revision,
            checkpoint: digest,
            destination_ready: digest,
        };
        let group = split_group_id(ClusterId(cluster), seal.operation);
        let destination = Delegation {
            namespace: seal.moved,
            partition: seal.destination,
            region: own.region,
            log_group: group,
            epoch: seal.next_epoch,
            activation: Some(fence),
        };
        // 1. The root grants the destination group (this node its single
        // voter), then the group hosts the sealed image under a root permit.
        let _ = std::io::Write::write_fmt(
            &mut std::io::stderr().lock(),
            format_args!(
                "TRACE {}\n",
                format!(
                    "continue_split hosted={} root_has_dest={}",
                    handles.directory.host_of(seal.destination).is_some(),
                    root.delegations.contains_key(&seal.moved.start)
                )
            ),
        );
        let Some(destination_host) = handles.directory.host_of(seal.destination) else {
            let plan = PartitionPlan::split_destination(cluster, node, destination, &image)?;
            let derived = next_first_directory_command(plan, observation, now, &self.budget);
            let _ = std::io::Write::write_fmt(
                &mut std::io::stderr().lock(),
                format_args!(
                    "TRACE {}\n",
                    format!(
                        "grant derivation {:?}",
                        derived
                            .as_ref()
                            .map(|c| c.is_some())
                            .map_err(|e| format!("{e:?}"))
                    )
                ),
            );
            match derived {
                Ok(Some(command)) => {
                    let journals = self.journals.as_mut().ok_or(AgentError::Identity)?;
                    journals.root.intend(&handles.control, command).await?;
                    return Ok(Some(AgentStep::Advanced));
                }
                Ok(None) => {}
                Err(
                    crate::directory_bootstrap::DirectoryBootstrapError::NotReady
                    | crate::directory_bootstrap::DirectoryBootstrapError::Capacity,
                ) => return Ok(Some(AgentStep::Idle)),
                Err(error) => return Err(error.into()),
            }
            handles.directory.request(HostRequest::Host {
                plan: Box::new(plan),
                image: Box::new(image),
            })?;
            return Ok(Some(AgentStep::Advanced));
        };
        let progress = destination_host.progress();
        if progress.stopped {
            return Err(AgentError::Stopped);
        }
        if progress.applied_index == 0 || progress.leader != node {
            return Ok(Some(AgentStep::Idle));
        }
        // 2. The root commits the split under both groups' signatures.
        match root.delegations.get(&seal.moved.start) {
            None => {
                let proofs = self
                    .delegation_proofs(handles, own.log_group, group, fence, now)
                    .await?;
                let mut proposed = destination;
                proposed.activation = None;
                let command = ControlCommand::VerifiedRoot(VerifiedRootCommand {
                    command: RootCommand {
                        expected_revision: root.revision,
                        operation: RootOperation::Split {
                            start: own.namespace.start,
                            at: seal.moved.start,
                            expected_epoch: own.epoch,
                            destination: proposed,
                            fence,
                        },
                    },
                    evidence: evidence_from(proofs, now),
                });
                let journals = self.journals.as_mut().ok_or(AgentError::Identity)?;
                journals.root.intend(&handles.control, command).await?;
                Ok(Some(AgentStep::Advanced))
            }
            Some(existing)
                if existing.partition == seal.destination && existing.epoch == seal.next_epoch =>
            {
                // 3. The destination installs the delegation as its first
                // command; it is observed once this node leads it.
                let Some((state, snapshot, installed)) =
                    observed_partition(observed, seal.destination)
                else {
                    return Ok(Some(AgentStep::Idle));
                };
                if state.sealed.is_some() {
                    let command = ControlCommand::VerifiedPartition(VerifiedPartitionCommand {
                        command: PartitionCommand {
                            expected_revision: state.revision,
                            delegation_epoch: state.delegation.epoch,
                            operation: PartitionOperation::Install {
                                delegation: *existing,
                            },
                        },
                        evidence: control_evidence(installed, cluster, now, &self.budget)?,
                    });
                    if snapshot.revisions.partition != state.revision {
                        return Err(AgentError::Identity);
                    }
                    self.current_partition = Some(seal.destination);
                    let step = self.intend_partition(handles, command).await;
                    self.current_partition = Some(own.partition);
                    return step.map(Some);
                }
                // 4. The source releases what left.
                let kept = root
                    .delegations
                    .get(&own.namespace.start)
                    .filter(|kept| kept.partition == own.partition && kept.epoch == seal.next_epoch)
                    .ok_or(AgentError::Identity)?;
                let (_, source_snapshot, source_installed) =
                    observed_partition(observed, own.partition).ok_or(AgentError::Identity)?;
                let command = ControlCommand::VerifiedPartition(VerifiedPartitionCommand {
                    command: PartitionCommand {
                        expected_revision: directory.revision,
                        delegation_epoch: own.epoch,
                        operation: PartitionOperation::Release { delegation: *kept },
                    },
                    evidence: control_evidence(source_installed, cluster, now, &self.budget)?,
                });
                if source_snapshot.revisions.partition != directory.revision {
                    return Err(AgentError::Identity);
                }
                self.intend_partition(handles, command).await.map(Some)
            }
            Some(_) => Err(AgentError::Identity),
        }
    }
    /// The merge destination takes the sealed upper partition the root
    /// already merged into its delegation, then retires that host.
    #[allow(
        clippy::too_many_arguments,
        reason = "one bounded pass over borrowed observations; no state is retained"
    )]
    async fn absorb(
        &mut self,
        handles: &NetworkHandles,
        pool: &PeerConnectionPool,
        delegation: &Delegation,
        directory: &PartitionCheckpoint,
        snapshot: &ControlSnapshot,
        installed: &ControlAuthoritySnapshot,
        now: i64,
    ) -> Result<AgentStep, AgentError> {
        let cluster = self.state.genesis.founder.cluster;
        let fence = delegation.activation.ok_or(AgentError::Identity)?;
        let Some(source) = handles.directory.host_of(fence.source) else {
            return Err(AgentError::Identity);
        };
        let client = Self::local_client(cluster, self.state.node);
        let namespace = handles.directory.namespace();
        let peer = self.peer(client, namespace)?;
        let Some((moved_snapshot, _)) = self
            .observe_partition(&PartitionAccess::Local(source), pool, namespace, peer)
            .await?
        else {
            return Ok(AgentStep::Idle);
        };
        let ControlBootstrap::Partition { directory: moved } = moved_snapshot.state else {
            return Err(AgentError::Identity);
        };
        if moved.sealed.is_none() {
            return Err(AgentError::Identity);
        }
        let command = ControlCommand::VerifiedPartition(VerifiedPartitionCommand {
            command: PartitionCommand {
                expected_revision: directory.revision,
                delegation_epoch: directory.delegation.epoch,
                operation: PartitionOperation::Absorb {
                    delegation: *delegation,
                    moved: Box::new(moved),
                },
            },
            evidence: control_evidence(installed, cluster, now, &self.budget)?,
        });
        if snapshot.revisions.partition != directory.revision {
            return Err(AgentError::Identity);
        }
        let step = self.intend_partition(handles, command).await?;
        // The merged-away partition is forgotten for restarts; its sealed
        // group keeps refusing until shutdown.
        handles.directory.request(HostRequest::Retire {
            partition: fence.source,
        })?;
        Ok(step)
    }
    /// Both groups' signatures over one delegation fence: this node signs as
    /// the single voter of each (partition groups live on the founder).
    async fn delegation_proofs(
        &mut self,
        handles: &NetworkHandles,
        source_group: LogGroupId,
        destination_group: LogGroupId,
        fence: DelegationFence,
        now: i64,
    ) -> Result<Vec<focal_directory::AuthorityProof>, AgentError> {
        let window = self.window(now)?;
        let source = handles
            .control
            .prepare_delegation_proof(source_group, fence, true, window)
            .await?
            .sign(&self.credentials)?;
        let destination = handles
            .control
            .prepare_delegation_proof(destination_group, fence, false, window)
            .await?
            .sign(&self.credentials)?;
        let mut proofs = Vec::new();
        proofs
            .try_reserve_exact(2)
            .map_err(|_| AgentError::Capacity)?;
        proofs.push(source.proof().clone());
        proofs.push(destination.proof().clone());
        Ok(proofs)
    }
}
