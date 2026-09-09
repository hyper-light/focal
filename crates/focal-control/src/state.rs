use crate::*;
use focal_directory::{
    AuthorityVerifier, ClusterId, DirectoryPartition, PartitionCheckpoint, PartitionId,
    PreparedPartitionUpdate, PreparedRootUpdate, RootCheckpoint, RootDirectory,
};
use focal_enrollment::{EnrollmentRegistry, PreparedEnrollmentUpdate};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlScope {
    Root,
    Partition(PartitionId),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlIdentity {
    pub cluster: ClusterId,
    pub group: [u8; 16],
    pub scope: ControlScope,
    pub genesis: [u8; 32],
}

/// Public committed bootstrap material, including only the public enrollment
/// CA and registry. Private signing keys and invitation secrets stay outside.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(
    clippy::large_enum_variant,
    reason = "one bounded bootstrap/checkpoint per metadata owner"
)]
pub enum ControlBootstrap {
    Root {
        directory: RootCheckpoint,
        enrollment: Vec<u8>,
    },
    Partition {
        directory: PartitionCheckpoint,
    },
}
/// The bootstrap shape written by control checkpoint schemas 1–3, whose
/// partition directory predates assignment progress.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(
    clippy::large_enum_variant,
    reason = "one bounded legacy checkpoint decoded per restore"
)]
pub(crate) enum LegacyControlBootstrap {
    Root {
        directory: RootCheckpoint,
        enrollment: Vec<u8>,
    },
    Partition {
        directory: focal_directory::PartitionCheckpointV1,
    },
}
impl TryFrom<LegacyControlBootstrap> for ControlBootstrap {
    type Error = ControlError;
    fn try_from(value: LegacyControlBootstrap) -> Result<Self, ControlError> {
        Ok(match value {
            LegacyControlBootstrap::Root {
                directory,
                enrollment,
            } => Self::Root {
                directory,
                enrollment,
            },
            LegacyControlBootstrap::Partition { directory } => Self::Partition {
                directory: directory
                    .try_into()
                    .map_err(|_| ControlError::Corrupt("legacy placement barrier"))?,
            },
        })
    }
}
impl ControlBootstrap {
    pub fn root(
        directory: &RootDirectory,
        enrollment: &EnrollmentRegistry,
    ) -> Result<Self, ControlError> {
        Ok(Self::Root {
            directory: directory.checkpoint().clone(),
            enrollment: enrollment.checkpoint()?,
        })
    }
    pub fn partition(directory: &DirectoryPartition) -> Self {
        Self::Partition {
            directory: directory.checkpoint().clone(),
        }
    }
    pub fn identity(&self, options: &ControlOptions) -> Result<ControlIdentity, ControlError> {
        let (cluster, scope) = match self {
            Self::Root { directory, .. } => (directory.cluster, ControlScope::Root),
            Self::Partition { directory } => {
                if directory.delegation.log_group.0 != options.consensus.group_id {
                    return Err(ControlError::WrongOwner);
                }
                (
                    directory.cluster,
                    ControlScope::Partition(directory.delegation.partition),
                )
            }
        };
        if cluster.0 != options.consensus.cluster_id {
            return Err(ControlError::WrongOwner);
        }
        Ok(ControlIdentity {
            cluster,
            group: options.consensus.group_id,
            scope,
            genesis: hash("focal.control.genesis.v1", self)?,
        })
    }
}

#[allow(
    clippy::large_enum_variant,
    reason = "one owned metadata machine per group"
)]
pub(crate) enum Machine {
    Root {
        directory: RootDirectory,
        enrollment: EnrollmentRegistry,
        _enrollment_charge: Allocation,
        contacts: Option<crate::contacts::NodeContacts>,
        authority: Option<crate::authority::InstalledAuthority>,
    },
    Partition {
        directory: DirectoryPartition,
        authority: Option<crate::authority::InstalledAuthority>,
    },
}
#[allow(
    clippy::large_enum_variant,
    reason = "one pre-admitted candidate per group"
)]
pub(crate) enum PreparedMachine {
    Membership,
    Contact {
        new: Option<crate::contacts::NodeContacts>,
        update: crate::contacts::PreparedNodeContact,
    },
    Root {
        update: PreparedRootUpdate,
        decided_at: Option<i64>,
    },
    Enrollment {
        update: PreparedEnrollmentUpdate,
        allocation: Allocation,
    },
    Partition {
        update: PreparedPartitionUpdate,
        decided_at: Option<i64>,
    },
    Activate {
        authority: crate::authority::InstalledAuthority,
        initial: Option<focal_directory::PreparedAuthorityUpdate>,
    },
    Authority {
        update: focal_directory::PreparedAuthorityUpdate,
        decided_at: i64,
    },
    Install(crate::authority::InstalledAuthority),
}
impl Machine {
    pub(crate) fn restore(
        state: ControlBootstrap,
        options: &ControlOptions,
        budget: &MemoryBudget,
    ) -> Result<Self, ControlError> {
        Ok(match state {
            ControlBootstrap::Root {
                directory,
                enrollment,
            } => {
                let _decode = budget.reserve(
                    BudgetKind::Recovery,
                    BudgetLane::Completion,
                    charge(enrollment.len().saturating_add(8192), 32)?,
                )?;
                let enrollment = EnrollmentRegistry::restore(
                    &enrollment,
                    options.consensus.cluster_id,
                    options.enrollment.clone(),
                )?;
                let allocation = budget
                    .reserve(
                        BudgetKind::Control,
                        BudgetLane::Completion,
                        charge(enrollment.charged_bytes(), 16)?,
                    )?
                    .commit();
                Self::Root {
                    directory: RootDirectory::restore(directory, options.root, budget.clone())?,
                    enrollment,
                    _enrollment_charge: allocation,
                    contacts: None,
                    authority: None,
                }
            }
            ControlBootstrap::Partition { directory } => Self::Partition {
                directory: DirectoryPartition::restore(
                    directory,
                    options.partition,
                    budget.clone(),
                )?,
                authority: None,
            },
        })
    }
    pub(crate) fn contacts(&self) -> Option<&ContactCheckpoint> {
        match self {
            Self::Root { contacts, .. } => contacts.as_ref().map(|rows| rows.checkpoint()),
            _ => None,
        }
    }
    pub(crate) fn contact_charge(&self) -> usize {
        match self {
            Self::Root { contacts, .. } => {
                contacts.as_ref().map_or(4096, |rows| rows.charged_bytes())
            }
            _ => 4096,
        }
    }
    pub(crate) fn restore_contacts(
        &mut self,
        state: ContactCheckpoint,
        options: &ControlOptions,
        budget: &MemoryBudget,
        index: u64,
    ) -> Result<(), ControlError> {
        if state.cluster != options.consensus.cluster_id {
            return Err(ControlError::WrongOwner);
        }
        let Self::Root { contacts, .. } = self else {
            return Err(ControlError::WrongOwner);
        };
        *contacts = Some(crate::contacts::NodeContacts::restore(
            state,
            options.contacts,
            budget.clone(),
            index,
        )?);
        Ok(())
    }
    pub(crate) fn authority(&self) -> Option<&crate::authority::InstalledAuthority> {
        match self {
            Self::Root { authority, .. } | Self::Partition { authority, .. } => authority.as_ref(),
        }
    }
    fn authority_slot(&mut self) -> &mut Option<crate::authority::InstalledAuthority> {
        match self {
            Self::Root { authority, .. } | Self::Partition { authority, .. } => authority,
        }
    }
    fn local_enrollment(&self) -> Option<&EnrollmentRegistry> {
        match self {
            Self::Root { enrollment, .. } => Some(enrollment),
            _ => None,
        }
    }
    fn namespace(&self) -> focal_directory::NamespaceRange {
        match self {
            Self::Root { .. } => focal_directory::NamespaceRange::all(),
            Self::Partition { directory, .. } => directory.checkpoint().delegation.namespace,
        }
    }
    pub(crate) fn restore_authority(
        &mut self,
        snapshot: ControlAuthoritySnapshot,
        identity: ControlIdentity,
        index: u64,
        options: &ControlOptions,
        budget: &MemoryBudget,
    ) -> Result<(), ControlError> {
        if snapshot.applied_index != index
            || snapshot.source_index > index && identity.scope == ControlScope::Root
        {
            return Err(ControlError::WrongOwner);
        }
        let authority = crate::authority::InstalledAuthority::from_snapshot(
            snapshot,
            identity,
            self.namespace(),
            options,
            budget,
            self.local_enrollment(),
            true,
        )?;
        *self.authority_slot() = Some(authority);
        Ok(())
    }
    pub(crate) fn export_authority(
        &self,
        identity: ControlIdentity,
        index: u64,
    ) -> Result<Option<ControlAuthoritySnapshot>, ControlError> {
        self.authority()
            .map(|authority| authority.export(identity, index, self.local_enrollment()))
            .transpose()
    }
    pub(crate) fn authority_estimate(&self) -> Result<usize, ControlError> {
        self.authority()
            .map(|authority| authority.estimate(self.local_enrollment()))
            .transpose()
            .map(|value| value.unwrap_or(0))
    }
    pub(crate) fn prepare(
        &self,
        command: &ControlCommand,
        command_bytes: usize,
        budget: &MemoryBudget,
        verifier: &impl AuthorityVerifier,
        identity: ControlIdentity,
        options: &ControlOptions,
    ) -> Result<PreparedMachine, ControlError> {
        match (self, command) {
            (
                Self::Root {
                    enrollment,
                    authority: Some(authority),
                    ..
                },
                ControlCommand::Membership(ControlMembershipCommand {
                    change: focal_consensus::MembershipChange::AddLearner { node },
                    ..
                }),
            ) => {
                // Re-evaluate this prerequisite at the serialized admission
                // point: a journaled intent cannot override a later quarantine
                // or enrollment revocation. Exact receipts resolve beforehand.
                let live = authority.registry.node(*node).is_some_and(|grant| {
                    grant.enrollment.eligible
                        && grant.expires_at > authority.decided_at
                        && enrollment.enrollments().any(|receipt| {
                            receipt.identity.role == focal_enrollment::EnrollmentRole::Node
                                && receipt.identity.node_id == Some(*node)
                                && receipt.identity.principal == grant.principal
                                && focal_model::ContentHash(receipt.public_key)
                                    == grant.enrollment.identity
                                && receipt.issued_at <= authority.decided_at
                                && receipt.expires_at > authority.decided_at
                                && matches!(
                                    enrollment.invitation_revoked(receipt.invitation),
                                    Ok(false)
                                )
                        })
                });
                if !live {
                    return Err(focal_directory::DirectoryError::CompareFailed.into());
                }
                Ok(PreparedMachine::Membership)
            }
            (_, ControlCommand::Membership(_)) => Ok(PreparedMachine::Membership),
            (
                Self::Root {
                    enrollment,
                    contacts,
                    ..
                },
                ControlCommand::NodeContact(command),
            ) => {
                let new = if contacts.is_none() {
                    Some(crate::contacts::NodeContacts::new(
                        identity.cluster.0,
                        options.contacts,
                        budget.clone(),
                    )?)
                } else {
                    None
                };
                let rows = contacts
                    .as_ref()
                    .or(new.as_ref())
                    .ok_or(ControlError::WrongOwner)?;
                let update = rows.prepare(command, enrollment)?;
                Ok(PreparedMachine::Contact { new, update })
            }
            (
                Self::Root {
                    directory,
                    authority: None,
                    ..
                },
                ControlCommand::Root(command),
            ) => Ok(PreparedMachine::Root {
                update: directory.prepare(command, verifier)?,
                decided_at: None,
            }),
            (
                Self::Partition {
                    directory,
                    authority: None,
                },
                ControlCommand::Partition(command),
            ) => Ok(PreparedMachine::Partition {
                update: directory.prepare(command, verifier)?,
                decided_at: None,
            }),
            (_, ControlCommand::Root(_) | ControlCommand::Partition(_))
                if self.authority().is_some() =>
            {
                Err(focal_directory::DirectoryError::UnverifiedAuthority.into())
            }
            (Self::Root { enrollment, .. }, ControlCommand::Enrollment(command)) => {
                let bytes = enrollment
                    .charged_bytes()
                    .checked_add(command_bytes)
                    .and_then(|n| n.checked_add(8192))
                    .ok_or(ControlError::Capacity)?;
                let allocation = budget
                    .reserve(
                        BudgetKind::Control,
                        BudgetLane::Completion,
                        charge(bytes, 16)?,
                    )?
                    .commit();
                let update = enrollment.prepare_command(command)?;
                if charge(update.charged_bytes(), 16)? > allocation.bytes() {
                    return Err(ControlError::Capacity);
                }
                Ok(PreparedMachine::Enrollment { update, allocation })
            }
            (
                Self::Root {
                    directory,
                    enrollment,
                    authority: None,
                    ..
                },
                ControlCommand::ActivateAuthority(AuthorityActivation::Root {
                    expected_root_revision,
                    expected_enrollment_revision,
                    decided_at,
                }),
            ) => {
                if directory.revision() != *expected_root_revision
                    || enrollment.revision() != *expected_enrollment_revision
                {
                    return Err(focal_directory::DirectoryError::CompareFailed.into());
                }
                let mut authority = crate::authority::InstalledAuthority::root(
                    identity, enrollment, options, budget,
                )?;
                let initial = authority.registry.prepare(
                    &crate::authority::activation_command(enrollment, *decided_at),
                    enrollment,
                )?;
                authority.decided_at = *decided_at;
                Ok(PreparedMachine::Activate {
                    authority,
                    initial: Some(initial),
                })
            }
            (
                Self::Partition {
                    directory,
                    authority: None,
                },
                ControlCommand::ActivateAuthority(AuthorityActivation::Partition {
                    expected_partition_revision,
                    snapshot,
                }),
            ) => {
                if directory.revision() != *expected_partition_revision {
                    return Err(focal_directory::DirectoryError::CompareFailed.into());
                }
                let authority = crate::authority::InstalledAuthority::from_snapshot(
                    snapshot.clone(),
                    identity,
                    self.namespace(),
                    options,
                    budget,
                    None,
                    false,
                )?;
                Ok(PreparedMachine::Activate {
                    authority,
                    initial: None,
                })
            }
            (_, ControlCommand::ActivateAuthority(_)) if self.authority().is_some() => {
                // Activation compares against absence. Exact committed retries
                // are resolved before preparation; a new activation intent has
                // lost its precondition and can safely refresh its plan.
                Err(focal_directory::DirectoryError::CompareFailed.into())
            }
            (
                Self::Root {
                    directory,
                    enrollment,
                    authority: Some(authority),
                    ..
                },
                ControlCommand::Authority(command),
            ) => {
                // A superseded durable intent must report its failed compare
                // before an advanced decision clock obscures that precondition.
                if command.expected_revision != authority.registry.revision()
                    || command.enrollment_revision != enrollment.revision()
                {
                    return Err(focal_directory::DirectoryError::CompareFailed.into());
                }
                authority.check_time(command.decided_at)?;
                if let focal_directory::AuthorityOperation::GrantNode { grant, .. } =
                    &command.operation
                    && grant.enrollment.region.0 != [0; 16]
                    && !directory
                        .checkpoint()
                        .regions
                        .get(&grant.enrollment.region)
                        .is_some_and(|region| {
                            region.authority_epoch == grant.enrollment.authority_epoch
                        })
                {
                    return Err(focal_directory::DirectoryError::UnverifiedAuthority.into());
                }
                Ok(PreparedMachine::Authority {
                    update: authority.registry.prepare(command, enrollment)?,
                    decided_at: command.decided_at,
                })
            }
            (
                Self::Root {
                    directory,
                    enrollment,
                    authority: Some(authority),
                    ..
                },
                ControlCommand::VerifiedRoot(command),
            ) => {
                // A newer directory decision can advance verification time
                // without changing the authority or enrollment revision.
                // Preserve the failed compare so saved intents can replan.
                if directory.revision() != command.command.expected_revision {
                    return Err(focal_directory::DirectoryError::CompareFailed.into());
                }
                let verifier = authority.verifier(Some(enrollment), &command.evidence)?;
                Ok(PreparedMachine::Root {
                    update: directory.prepare(&command.command, &verifier)?,
                    decided_at: Some(command.evidence.decided_at),
                })
            }
            (
                Self::Partition {
                    directory,
                    authority: Some(authority),
                },
                ControlCommand::VerifiedPartition(command),
            ) => {
                if directory.revision() != command.command.expected_revision {
                    return Err(focal_directory::DirectoryError::CompareFailed.into());
                }
                let verifier = authority.verifier(None, &command.evidence)?;
                Ok(PreparedMachine::Partition {
                    update: directory.prepare(&command.command, &verifier)?,
                    decided_at: Some(command.evidence.decided_at),
                })
            }
            (
                Self::Partition {
                    authority: Some(authority),
                    ..
                },
                ControlCommand::InstallAuthority(installation),
            ) => {
                let mut next = crate::authority::InstalledAuthority::from_snapshot(
                    installation.snapshot.clone(),
                    identity,
                    self.namespace(),
                    options,
                    budget,
                    None,
                    false,
                )?;
                authority.check_time(installation.decided_at)?;
                next.check_time(installation.decided_at)?;
                next.decided_at = installation.decided_at;
                authority.check_replacement(&next, installation.expected_source_index)?;
                Ok(PreparedMachine::Install(next))
            }
            _ => Err(ControlError::WrongOwner),
        }
    }
    pub(crate) fn publish(
        &mut self,
        prepared: PreparedMachine,
        index: u64,
    ) -> Result<(), ControlError> {
        match prepared {
            PreparedMachine::Membership => {}
            PreparedMachine::Contact { new, update } => {
                let Self::Root { contacts, .. } = self else {
                    return Err(ControlError::WrongOwner);
                };
                match new {
                    Some(mut rows) => {
                        rows.publish(update, index)?;
                        *contacts = Some(rows);
                    }
                    None => contacts
                        .as_mut()
                        .ok_or(ControlError::WrongOwner)?
                        .publish(update, index)?,
                }
            }
            PreparedMachine::Activate {
                mut authority,
                initial,
            } => {
                if self.authority().is_some() {
                    return Err(ControlError::WrongOwner);
                }
                if let Some(initial) = initial {
                    authority.registry.publish(initial, index)?;
                }
                *self.authority_slot() = Some(authority);
            }
            PreparedMachine::Install(authority) => {
                if !matches!(
                    self,
                    Self::Partition {
                        authority: Some(_),
                        ..
                    }
                ) {
                    return Err(ControlError::WrongOwner);
                }
                *self.authority_slot() = Some(authority);
            }
            PreparedMachine::Authority { update, decided_at } => {
                let authority = self
                    .authority_slot()
                    .as_mut()
                    .ok_or(ControlError::WrongOwner)?;
                authority.registry.publish(update, index)?;
                authority.decided_at = decided_at;
            }
            PreparedMachine::Root { update, decided_at } => {
                let Self::Root {
                    directory,
                    authority,
                    ..
                } = self
                else {
                    return Err(ControlError::WrongOwner);
                };
                directory.publish(update)?;
                if let (Some(authority), Some(time)) = (authority, decided_at) {
                    authority.decided_at = time;
                }
            }
            PreparedMachine::Partition { update, decided_at } => {
                let Self::Partition {
                    directory,
                    authority,
                } = self
                else {
                    return Err(ControlError::WrongOwner);
                };
                directory.publish(update)?;
                if let (Some(authority), Some(time)) = (authority, decided_at) {
                    authority.decided_at = time;
                }
            }
            PreparedMachine::Enrollment { update, allocation } => {
                let Self::Root {
                    enrollment,
                    _enrollment_charge,
                    ..
                } = self
                else {
                    return Err(ControlError::WrongOwner);
                };
                enrollment.publish(update, index)?;
                *_enrollment_charge = allocation;
            }
        }
        Ok(())
    }
    pub(crate) fn revisions(&self) -> ControlRevisions {
        match self {
            Self::Root {
                directory,
                enrollment,
                ..
            } => ControlRevisions {
                root: directory.revision(),
                enrollment: enrollment.revision(),
                partition: 0,
            },
            Self::Partition { directory, .. } => ControlRevisions {
                partition: directory.revision(),
                ..Default::default()
            },
        }
    }
    pub(crate) fn export(&self) -> Result<ControlBootstrap, ControlError> {
        match self {
            Self::Root {
                directory,
                enrollment,
                ..
            } => ControlBootstrap::root(directory, enrollment),
            Self::Partition { directory, .. } => Ok(ControlBootstrap::partition(directory)),
        }
    }
    pub(crate) fn export_estimate(&self) -> Result<usize, ControlError> {
        let (directory, enrollment) = match self {
            Self::Root {
                directory,
                enrollment,
                ..
            } => (
                postcard::experimental::serialized_size(directory.checkpoint())?,
                enrollment.charged_bytes(),
            ),
            Self::Partition { directory, .. } => (
                postcard::experimental::serialized_size(directory.checkpoint())?,
                0,
            ),
        };
        directory
            .checked_add(enrollment)
            .and_then(|n| n.checked_add(8192))
            .ok_or(ControlError::Capacity)
    }
}

#[cfg(test)]
mod legacy_tests {
    use super::*;
    use focal_directory::{
        Delegation, LogGroupId, NamespaceRange, PartitionCheckpointV1, PartitionConfig,
        PartitionId, RegionId, RootConfig,
    };
    use focal_memory::MemoryBudget;
    use std::collections::BTreeMap;

    const CLUSTER: [u8; 16] = [7; 16];

    #[test]
    fn legacy_root_bootstraps_keep_their_bytes_and_partitions_convert_to_schema_two() {
        let budget = MemoryBudget::new(8 * 1024 * 1024, 1024 * 1024).unwrap();
        let root =
            RootDirectory::new(ClusterId(CLUSTER), RootConfig::default(), budget.clone()).unwrap();
        let current = ControlBootstrap::Root {
            directory: root.checkpoint().clone(),
            enrollment: vec![4, 5],
        };
        let legacy = LegacyControlBootstrap::Root {
            directory: root.checkpoint().clone(),
            enrollment: vec![4, 5],
        };
        assert_eq!(
            postcard::to_stdvec(&legacy).unwrap(),
            postcard::to_stdvec(&current).unwrap()
        );
        assert_eq!(ControlBootstrap::try_from(legacy).unwrap(), current);

        let delegation = Delegation {
            namespace: NamespaceRange::all(),
            partition: PartitionId::from_u128(3),
            region: RegionId::from_u128(1),
            log_group: LogGroupId::from_u128(9),
            epoch: 1,
            activation: None,
        };
        let legacy = LegacyControlBootstrap::Partition {
            directory: PartitionCheckpointV1 {
                schema: 1,
                cluster: ClusterId(CLUSTER),
                delegation,
                revision: 0,
                sealed: None,
                nodes: BTreeMap::new(),
                sessions: BTreeMap::new(),
            },
        };
        let legacy_bytes = postcard::to_stdvec(&legacy).unwrap();
        let converted = ControlBootstrap::try_from(legacy).unwrap();
        let ControlBootstrap::Partition { directory } = &converted else {
            panic!("partition bootstrap");
        };
        assert_eq!(
            directory.schema,
            focal_directory::PARTITION_CHECKPOINT_SCHEMA
        );
        DirectoryPartition::restore(directory.clone(), PartitionConfig::default(), budget).unwrap();
        // The genesis identity hashes the bootstrap bytes, so a partition group
        // bootstrapped at schema 1 is not the group bootstrapped at schema 2.
        assert_ne!(legacy_bytes, postcard::to_stdvec(&converted).unwrap());
        let fresh = ControlBootstrap::partition(
            &DirectoryPartition::new(
                ClusterId(CLUSTER),
                delegation,
                PartitionConfig::default(),
                MemoryBudget::new(8 * 1024 * 1024, 1024 * 1024).unwrap(),
            )
            .unwrap(),
        );
        assert_eq!(fresh, converted);
    }
}
