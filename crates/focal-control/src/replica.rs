use crate::{
    retry::{RetryCheckpoint, RetryState},
    state::{Machine, PreparedMachine},
    *,
};
use focal_consensus::{
    DurableNode, FaultPoint, Message, NodeStatus, ReadBarrier, SharedWal, StateRole,
};
use focal_directory::{AuthorityVerifier, DirectoryPartition, RootDirectory};
use focal_enrollment::EnrollmentRegistry;
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use std::{collections::BTreeMap, path::Path};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CommandEnvelope {
    schema: u16,
    identity: ControlIdentity,
    owner_node: u64,
    owner_term: u64,
    request: ControlRequest,
}
/// Checkpoint schemas 1–3 carry the schema 1 partition directory; schema 4
/// carries the current one. The layouts are otherwise identical, so the
/// state type is the only parameter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Checkpoint<S = ControlBootstrap> {
    schema: u16,
    identity: ControlIdentity,
    applied_index: u64,
    state: S,
    retries: RetryCheckpoint,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CheckpointV2<S = ControlBootstrap> {
    schema: u16,
    identity: ControlIdentity,
    applied_index: u64,
    state: S,
    retries: RetryCheckpoint,
    authority: ControlAuthoritySnapshot,
}
/// The shape schemas 3 and 4 wrote: contacts without topology labels.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CheckpointV3<S = ControlBootstrap> {
    schema: u16,
    identity: ControlIdentity,
    applied_index: u64,
    state: S,
    retries: RetryCheckpoint,
    authority: Option<ControlAuthoritySnapshot>,
    configuration_index: u64,
    contacts: Option<crate::contacts::ContactCheckpointV1>,
}
/// The shape schema 5 wrote: contacts without advertised names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CheckpointV5<S = ControlBootstrap> {
    schema: u16,
    identity: ControlIdentity,
    applied_index: u64,
    state: S,
    retries: RetryCheckpoint,
    authority: Option<ControlAuthoritySnapshot>,
    configuration_index: u64,
    contacts: Option<crate::contacts::ContactCheckpointV2>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CheckpointV6<S = ControlBootstrap> {
    schema: u16,
    identity: ControlIdentity,
    applied_index: u64,
    state: S,
    retries: RetryCheckpoint,
    authority: Option<ControlAuthoritySnapshot>,
    configuration_index: u64,
    contacts: Option<crate::contacts::ContactCheckpointV3>,
}
/// Schema 7: contacts carry retirement counters (24 §19).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CheckpointV7<S = ControlBootstrap> {
    schema: u16,
    identity: ControlIdentity,
    applied_index: u64,
    state: S,
    retries: RetryCheckpoint,
    authority: Option<ControlAuthoritySnapshot>,
    configuration_index: u64,
    contacts: Option<ContactCheckpoint>,
}
/// Schema 8: the record of the latest configuration change, so a restarted
/// replica still attests it (the audit's F24).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CheckpointV8<S = ControlBootstrap> {
    schema: u16,
    identity: ControlIdentity,
    applied_index: u64,
    state: S,
    retries: RetryCheckpoint,
    authority: Option<ControlAuthoritySnapshot>,
    configuration_index: u64,
    contacts: Option<ContactCheckpoint>,
    membership: Option<ControlMembershipRecord>,
}
const CHECKPOINT_SCHEMA: u16 = 8;
const COMMAND_SCHEMA: u16 = 2;
/// A delivery the node handed over, continued from where it stands: the
/// events, the output built so far, and the cursors past what is applied.
/// A refusal that changed nothing of the node — memory for an entry's
/// decode, a snapshot's restore — leaves it here for the next drain; the
/// replica held one such delivery per poll and failed on it before
/// (a member brought up by snapshot on a loaded runner, ubuntu CI,
/// 2026-10-03). One at most: the node is not asked for more while one is held.
struct RetainedDelivery {
    events: focal_consensus::NodeEvents,
    output: ControlEvents,
    entry: usize,
    configuration: usize,
    snapshot_done: bool,
}
impl RetainedDelivery {
    fn new(mut events: focal_consensus::NodeEvents) -> Self {
        Self {
            output: ControlEvents {
                allocation: events.take_allocation(),
                ..Default::default()
            },
            events,
            entry: 0,
            configuration: 0,
            snapshot_done: false,
        }
    }
}
/// One entry of a delivery as it is applied: read in place.
struct DeliveredEntry<'a> {
    index: u64,
    term: u64,
    data: &'a [u8],
}
struct Pending {
    request: ControlRequestId,
    request_hash: [u8; 32],
    envelope_hash: [u8; 32],
    membership: Option<PreparedConfiguration>,
    term: u64,
    machine: PreparedMachine,
    retries: RetryState,
    _allocation: Allocation,
}
struct PreparedConfiguration {
    index: u64,
    before: MembershipConfiguration,
    after: MembershipConfiguration,
}

#[derive(Debug, Default)]
pub struct ControlEvents {
    /// Bound these in the host transport queue; consensus ownership and peer
    /// identity must be authenticated before calling step/step_authenticated.
    pub messages: Vec<Message>,
    pub read_states: Vec<ReadBarrier>,
    pub completed: Option<ControlReceipt>,
    /// Leadership/snapshot replacement released this local preparation. This
    /// is an unknown outcome: retry/query its ID, never invent a replacement ID.
    pub uncertain: Option<ControlRequestId>,
    pub applied_index: u64,
    allocation: Option<Allocation>,
    /// The charge of the held barriers this delivery let go.
    parked: Option<Allocation>,
}
impl ControlEvents {
    /// Move with messages/read barriers transferred into the host transport.
    pub fn take_allocation(&mut self) -> Option<Allocation> {
        self.allocation.take()
    }
}

/// A single mutable metadata owner. Independent instances may share one node
/// WAL while retaining independent consensus, admission, and retry windows.
pub struct ControlReplica {
    node: DurableNode,
    options: ControlOptions,
    identity: ControlIdentity,
    machine: Machine,
    retries: RetryState,
    budget: MemoryBudget,
    pending: Option<Pending>,
    applied_index: u64,
    configuration_index: u64,
    /// The entry that last changed the configuration, once one has.
    membership: Option<ControlMembershipRecord>,
    drained: bool,
    failed: bool,
    /// Why the replica failed, where it did: the first error that stopped
    /// it, named by its text. A replica that failed answers every call
    /// `Failed`; without this the cause was gone with the first answer (a
    /// member brought up by snapshot reported the egress's end alone,
    /// macOS and ubuntu CI, 2026-10-02 and 2026-10-03).
    failure: Option<String>,
    /// A delivery a refusal stopped, continued by the next drain.
    retained: Option<RetainedDelivery>,
    /// Read barriers answered above what this replica has applied — a
    /// follower's read, answered with its leader's commit (27 §5) — held
    /// until the entries they name are applied; no more than the reads the
    /// core holds in flight, their bytes charged to `parked_charge` while
    /// held.
    parked_reads: Vec<ReadBarrier>,
    parked_charge: Option<Allocation>,
    /// Barriers held so, and barriers dropped at that bound, since open.
    reads_parked: u64,
    reads_dropped: u64,
    /// Founded here on a sealed image (`ControlBootstrap::is_image`).
    founded_from_image: bool,
}
impl ControlReplica {
    pub fn open(
        options: ControlOptions,
        bootstrap: ControlBootstrap,
        budget: MemoryBudget,
        directory: impl AsRef<Path>,
    ) -> Result<Self, ControlError> {
        options.validate()?;
        let identity = bootstrap.identity(&options)?;
        let founded_from_image = bootstrap.is_image();
        let machine = Machine::restore(bootstrap, &options, &budget)?;
        let retries = RetryState::restore(BTreeMap::new(), &options.limits, &budget, 0)?;
        let mut node = DurableNode::open_in(options.consensus.clone(), directory, &budget)?;
        // What a control group applies — who is enrolled, the fence a binary
        // serves under, where a ledger is placed — its members act on when
        // they next start, before the group tells them anything.
        node.apply_on_written_commit();
        Ok(Self {
            node,
            options,
            identity,
            machine,
            retries,
            budget,
            pending: None,
            applied_index: 0,
            configuration_index: 0,
            membership: None,
            drained: false,
            failed: false,
            failure: None,
            retained: None,
            parked_reads: Vec::new(),
            parked_charge: None,
            reads_parked: 0,
            reads_dropped: 0,
            founded_from_image,
        })
    }
    pub fn open_on_wal(
        options: ControlOptions,
        bootstrap: ControlBootstrap,
        budget: MemoryBudget,
        wal: SharedWal,
    ) -> Result<Self, ControlError> {
        Self::open_with(options, bootstrap, budget, |config, budget| {
            DurableNode::open_on_wal_in(config, wal, budget)
        })
    }

    /// A control group's member over hyper-durable's shell (27 §15.7): its log one group of the
    /// node's hyper-log log, its records and image under the data directory, its disk charged to
    /// the envelope, all as `storage` names them. A control group's entries need no decoder beyond its baseline, so no
    /// write is held for a record. It applies on a commit its log holds, as over focal-log
    /// (`StateMachine::acts_at_start` for every entry, 27 §15.6).
    pub fn open_on_shell(
        options: ControlOptions,
        bootstrap: ControlBootstrap,
        budget: MemoryBudget,
        storage: &focal_consensus::ShellStorage,
    ) -> Result<Self, ControlError> {
        Self::open_with(options, bootstrap, budget, |config, budget| {
            DurableNode::open_on_shell(config, storage, budget, |_| None)
        })
    }

    fn open_with(
        options: ControlOptions,
        bootstrap: ControlBootstrap,
        budget: MemoryBudget,
        open: impl FnOnce(
            focal_consensus::NodeConfig,
            &MemoryBudget,
        ) -> Result<DurableNode, focal_consensus::ConsensusError>,
    ) -> Result<Self, ControlError> {
        options.validate()?;
        let identity = bootstrap.identity(&options)?;
        let founded_from_image = bootstrap.is_image();
        let machine = Machine::restore(bootstrap, &options, &budget)?;
        let retries = RetryState::restore(BTreeMap::new(), &options.limits, &budget, 0)?;
        let mut node = open(options.consensus.clone(), &budget)?;
        // As `open`: a control group applies on a commit its log holds.
        node.apply_on_written_commit();
        Ok(Self {
            node,
            options,
            identity,
            machine,
            retries,
            budget,
            pending: None,
            applied_index: 0,
            configuration_index: 0,
            membership: None,
            drained: false,
            failed: false,
            failure: None,
            retained: None,
            parked_reads: Vec::new(),
            parked_charge: None,
            reads_parked: 0,
            reads_dropped: 0,
            founded_from_image,
        })
    }
    pub fn identity(&self) -> ControlIdentity {
        self.identity
    }
    /// The fields beyond raft-rs's this group's member reads and writes from now on
    /// (`focal_consensus::Wire`): raised by focal-node once the upgrade fence opens
    /// `RAFT_KEPT_LEVEL`, before a member opened under that fence sends. Under `Wire::Kept` the
    /// member reads a refusal's `kept` and `lost` and keeps what arrives ahead of a hole (R17).
    pub fn set_raft_wire(&mut self, wire: focal_consensus::Wire) -> Result<(), ControlError> {
        self.node.set_raft_wire(wire)?;
        Ok(())
    }
    /// What this group's messages are encoded under (`focal_consensus::encode_message_in`).
    pub fn wire(&self) -> focal_consensus::Wire {
        self.node.wire()
    }
    /// The bounds this group's core holds its queues to (`focal_consensus::CoreLimits`).
    pub fn core_limits(&self) -> focal_consensus::CoreLimits {
        self.node.limits()
    }
    pub fn status(&self) -> NodeStatus {
        self.node.status()
    }
    pub fn peer_progress(&self) -> Vec<focal_consensus::PeerProgress> {
        self.node.peer_progress()
    }
    pub fn applied_index(&self) -> u64 {
        self.applied_index
    }
    /// Whether this replica's group was founded here on a sealed image (a
    /// split destination's founder): its log begins after the image, so it
    /// compacts at founding and no member is ever sent the image's prefix.
    pub fn founded_from_image(&self) -> bool {
        self.founded_from_image
    }
    /// The compaction floor: the index of the most recent installed snapshot,
    /// or zero if the log has never been compacted.
    pub fn snapshot_index(&self) -> u64 {
        self.node.snapshot_index()
    }
    /// The Raft index of the most recent committed membership change. A stored
    /// snapshot older than this carries a configuration that excludes members
    /// added since, so it cannot catch such a member up.
    /// Ticks without leader contact before this replica campaigns.
    pub fn election_tick(&self) -> usize {
        self.node.election_tick()
    }
    /// The ticks the replica waits beyond its election timeout before it
    /// campaigns (`DurableNode::set_patience`).
    pub fn set_patience(&mut self, ticks: usize) -> Result<(), ControlError> {
        self.check()?;
        self.node.set_patience(ticks)?;
        Ok(())
    }
    pub fn configuration_index(&self) -> u64 {
        self.configuration_index
    }
    pub fn membership_configuration(&self) -> MembershipConfiguration {
        self.node.membership_configuration()
    }
    pub fn has_pending(&self) -> bool {
        self.pending.is_some()
    }
    /// The request that holds this owner's one proposal, if one does:
    /// another is refused `Busy` until it is decided, and an owner that
    /// serves callers keeps theirs until then instead of handing them the
    /// refusal.
    pub fn pending_request(&self) -> Option<ControlRequestId> {
        self.pending.as_ref().map(|pending| pending.request)
    }
    pub fn revisions(&self) -> ControlRevisions {
        self.machine.revisions()
    }
    pub fn limits(&self) -> &ControlLimits {
        &self.options.limits
    }
    /// Reserve this amount before retaining a read candidate or exporting state.
    pub fn read_charge(&self, query: &ControlRead) -> Result<usize, ControlError> {
        let bytes = match query {
            ControlRead::State => self.machine.export_estimate()?,
            ControlRead::Receipt(_) => 4096,
            ControlRead::Membership => 2048 * 16 + 4096,
            ControlRead::Authority => self.machine.authority_estimate()?,
            ControlRead::Configuration | ControlRead::MembershipRecord => 2048 * 16 + 4096,
            ControlRead::Contacts => self.machine.contact_charge(),
            ControlRead::InvitationPage { limit, .. } => {
                if *limit == 0 || *limit > 64 {
                    return Err(ControlError::Invalid);
                }
                usize::from(*limit)
                    .checked_mul(1024)
                    .ok_or(ControlError::Capacity)?
            }
            ControlRead::Invitation { .. } | ControlRead::PrepareRevocation { .. } => 4096,
            ControlRead::PrepareEligibility { .. } => 8192,
            ControlRead::Route { .. } => 4096,
            ControlRead::RouteChanges { .. } => self.machine.route_log_charge(),
            ControlRead::AdminReceipt { .. } => 2048 * 16 + 4096,
            ControlRead::StateAndAuthority => {
                return self
                    .read_charge(&ControlRead::State)?
                    .checked_add(self.read_charge(&ControlRead::Authority)?)
                    .ok_or(ControlError::Capacity);
            }
        };
        charge(bytes.checked_add(4096).ok_or(ControlError::Capacity)?, 4)
    }
    /// Local committed data only. A host claiming a linearizable result must
    /// first complete a matching ReadIndex. Every export retains its read
    /// allocation through consumption or encoding/delivery.
    pub fn read_local(&self, query: &ControlRead) -> Result<ControlReadResult, ControlError> {
        self.check()?;
        if !self.drained {
            return Err(ControlError::NotReady);
        }
        match query {
            ControlRead::AdminReceipt { id } => Ok(ControlReadResult::AdminReceipt {
                configuration: self.configuration(),
                enrollment_revision: self
                    .enrollment()
                    .ok_or(ControlError::WrongOwner)?
                    .revision(),
                receipt: self.receipt(*id)?,
            }),
            ControlRead::InvitationPage {
                after,
                limit,
                expected_revision,
            } => {
                let registry = self.enrollment().ok_or(ControlError::WrongOwner)?;
                if expected_revision.is_some_and(|revision| revision != registry.revision()) {
                    return Err(focal_directory::DirectoryError::CompareFailed.into());
                }
                let (entries, next) = registry.invitation_page(*after, *limit)?;
                Ok(ControlReadResult::Invitations {
                    identity: self.identity,
                    applied_index: self.applied_index,
                    revision: registry.revision(),
                    entries,
                    next,
                })
            }
            ControlRead::Invitation { id } => {
                let registry = self.enrollment().ok_or(ControlError::WrongOwner)?;
                let mut entries = Vec::new();
                entries
                    .try_reserve_exact(1)
                    .map_err(|_| ControlError::Capacity)?;
                if let Some(value) = registry.invitation_status(*id) {
                    entries.push(value);
                }
                Ok(ControlReadResult::Invitations {
                    identity: self.identity,
                    applied_index: self.applied_index,
                    revision: registry.revision(),
                    entries,
                    next: None,
                })
            }
            ControlRead::PrepareRevocation { id } => {
                let registry = self.enrollment().ok_or(ControlError::WrongOwner)?;
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|_| ControlError::NotReady)?
                    .as_secs();
                let command = registry
                    .prepare_revoke(*id, i64::try_from(now).map_err(|_| ControlError::NotReady)?)?;
                Ok(ControlReadResult::PreparedRevocation {
                    identity: self.identity,
                    applied_index: self.applied_index,
                    invitation: *id,
                    command,
                })
            }
            ControlRead::PrepareEligibility { node, eligible } => {
                let registry = self.enrollment().ok_or(ControlError::WrongOwner)?;
                let authority = self.authority().ok_or(ControlError::NotReady)?;
                let grant = authority.node(*node).ok_or(ControlError::Invalid)?;
                if grant.enrollment.eligible == *eligible {
                    return Ok(ControlReadResult::PreparedEligibility {
                        identity: self.identity,
                        applied_index: self.applied_index,
                        node: *node,
                        generation: grant.enrollment.generation,
                        eligible: *eligible,
                        command: None,
                    });
                }
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|_| ControlError::NotReady)?
                    .as_secs();
                let generation = grant
                    .enrollment
                    .generation
                    .checked_add(1)
                    .ok_or(ControlError::Capacity)?;
                // The same enrollment at its next generation with the requested
                // eligibility; the attestation is the registry's to set.
                let mut enrollment = grant.enrollment.clone();
                enrollment.generation = generation;
                enrollment.eligible = *eligible;
                enrollment.attestation = focal_model::ContentHash([0; 32]);
                let command = focal_directory::AuthorityCommand {
                    expected_revision: authority.revision(),
                    enrollment_revision: registry.revision(),
                    decided_at: i64::try_from(now).map_err(|_| ControlError::NotReady)?,
                    operation: focal_directory::AuthorityOperation::GrantNode {
                        grant: focal_directory::NodeTopologyGrant {
                            enrollment,
                            principal: grant.principal,
                            expires_at: grant.expires_at,
                        },
                        expected_generation: Some(grant.enrollment.generation),
                    },
                };
                Ok(ControlReadResult::PreparedEligibility {
                    identity: self.identity,
                    applied_index: self.applied_index,
                    node: *node,
                    generation,
                    eligible: *eligible,
                    command: Some(Box::new(command)),
                })
            }
            ControlRead::MembershipRecord => {
                Ok(ControlReadResult::MembershipRecord(self.membership.clone()))
            }
            ControlRead::Configuration => {
                Ok(ControlReadResult::Configuration(self.configuration()))
            }
            ControlRead::Contacts if self.identity.scope == ControlScope::Root => {
                Ok(ControlReadResult::Contacts(ContactSnapshot {
                    identity: self.identity,
                    applied_index: self.applied_index,
                    contacts: self
                        .machine
                        .contacts()
                        .cloned()
                        .unwrap_or(ContactCheckpoint {
                            schema: crate::contacts::CONTACT_CHECKPOINT_SCHEMA,
                            cluster: self.identity.cluster.0,
                            revision: 0,
                            applied_index: 0,
                            records: Vec::new(),
                            retired_mutations: 0,
                            last_retirement_index: 0,
                        }),
                }))
            }
            ControlRead::Contacts => Err(ControlError::WrongOwner),
            ControlRead::Route { ledger } => {
                let partition = self.partition().ok_or(ControlError::WrongOwner)?;
                let epoch = partition.checkpoint().delegation.epoch;
                match partition.lookup(*ledger, epoch) {
                    Ok(route) => Ok(ControlReadResult::Route(Some(route))),
                    Err(
                        focal_directory::DirectoryError::Missing
                        | focal_directory::DirectoryError::OutsideNamespace,
                    ) => Ok(ControlReadResult::Route(None)),
                    Err(error) => Err(error.into()),
                }
            }
            ControlRead::RouteChanges { after_revision } => {
                let partition = self.partition().ok_or(ControlError::WrongOwner)?;
                Ok(ControlReadResult::RouteChanges(
                    partition.route_changes(*after_revision),
                ))
            }
            ControlRead::State => Ok(ControlReadResult::State(ControlSnapshot {
                identity: self.identity,
                applied_index: self.applied_index,
                revisions: self.machine.revisions(),
                state: self.machine.export()?,
            })),
            ControlRead::Receipt(id) => Ok(ControlReadResult::Receipt(self.receipt(*id)?)),
            ControlRead::Authority => Ok(ControlReadResult::Authority(
                self.machine
                    .export_authority(self.identity, self.applied_index)?,
            )),
            ControlRead::StateAndAuthority => Ok(ControlReadResult::StateAndAuthority {
                snapshot: Box::new(ControlSnapshot {
                    identity: self.identity,
                    applied_index: self.applied_index,
                    revisions: self.machine.revisions(),
                    state: self.machine.export()?,
                }),
                authority: self
                    .machine
                    .export_authority(self.identity, self.applied_index)?,
            }),
            ControlRead::Membership => {
                let status = self.node.status();
                Ok(ControlReadResult::Membership(ControlMembership {
                    node: status.node_id,
                    leader: status.leader_id,
                    term: status.term,
                    voters: status.voters,
                    learners: status.learners,
                    applied_index: self.applied_index,
                }))
            }
        }
    }
    /// These are local committed views after drain, not quorum-read grants.
    /// Complete a matching ReadIndex barrier before claiming linearizability.
    pub fn root(&self) -> Option<&RootDirectory> {
        match &self.machine {
            Machine::Root { directory, .. } => Some(directory),
            _ => None,
        }
    }
    pub fn enrollment(&self) -> Option<&EnrollmentRegistry> {
        match &self.machine {
            Machine::Root { enrollment, .. } => Some(enrollment),
            _ => None,
        }
    }
    /// The enrollment registry this owner authorizes peers against: its own
    /// for the root, the installed authority's copy for a partition.
    pub fn installed_enrollment(&self) -> Option<&EnrollmentRegistry> {
        self.machine
            .authority()
            .and_then(|authority| authority.enrollment(self.enrollment()).ok())
            .or_else(|| self.enrollment())
    }
    pub fn partition(&self) -> Option<&DirectoryPartition> {
        match &self.machine {
            Machine::Partition { directory, .. } => Some(directory),
            _ => None,
        }
    }
    /// Local installed state only; network readers still require ReadIndex.
    pub fn authority(&self) -> Option<&focal_directory::AuthorityRegistry> {
        self.machine
            .authority()
            .map(|authority| &authority.registry)
    }
    pub fn contacts(&self) -> Option<&ContactCheckpoint> {
        self.machine.contacts()
    }
    pub fn configuration(&self) -> ControlConfiguration {
        ControlConfiguration {
            identity: self.identity,
            applied_index: self.applied_index,
            configuration_index: self.configuration_index,
            configuration: self.node.membership_configuration(),
        }
    }
    /// The entry that last changed the configuration, as applied here.
    pub fn membership_record(&self) -> Option<&ControlMembershipRecord> {
        self.membership.as_ref()
    }
    pub fn accepts_peer(&self, node: u64) -> bool {
        self.node.membership_configuration().contains(node)
    }
    pub fn transfer(&mut self, request: &ControlTransfer) -> Result<(), ControlError> {
        // A replica that does not lead may still ask to lead itself: the
        // core forwards the request to the leader, which hands over as it
        // would to any transferee (a voter's administrator reaching only
        // that voter takes leadership away from a node that is leaving);
        // asked for any other target it answers who leads.
        if self.node.status().role == StateRole::Leader {
            self.check_ready()?;
        } else {
            self.check()?;
        }
        if self.pending.is_some() {
            return Err(ControlError::Busy);
        }
        if request.expected_configuration_index != self.configuration_index
            || request.expected != self.node.membership_configuration()
        {
            return Err(focal_directory::DirectoryError::CompareFailed.into());
        }
        self.node.transfer_leader(request.target)?;
        Ok(())
    }
    /// Hand leadership to `target` for this replica's own planned stop: no
    /// fence, since the owner is not carrying out a placement decision but
    /// leaving, and the group is better led by any live voter than by none.
    pub fn hand_off(&mut self, target: u64) -> Result<(), ControlError> {
        self.check_ready()?;
        self.node.transfer_leader(target)?;
        Ok(())
    }
    /// The voter to hand leadership to when this leader stops: the most
    /// caught-up one that is heard from, none while this replica does not
    /// lead or no other voter is.
    pub fn heir(&self) -> Option<u64> {
        let status = self.node.status();
        if status.role != focal_consensus::StateRole::Leader {
            return None;
        }
        crate::heir(&status, &self.node.peer_progress())
    }
    pub fn receipt(&self, id: ControlRequestId) -> Result<Option<ControlReceipt>, ControlError> {
        self.check()?;
        if !self.drained {
            return Err(ControlError::NotReady);
        }
        self.retries.lookup(id)
    }
    /// Local committed retry position for a trusted component's stable client.
    /// Resolve recovery and any outstanding intent before deriving its successor.
    pub fn latest_receipt(&self, client: [u8; 16]) -> Result<Option<ControlReceipt>, ControlError> {
        self.check()?;
        if !self.drained {
            return Err(ControlError::NotReady);
        }
        self.retries.latest(client)
    }
    pub fn campaign(&mut self) -> Result<(), ControlError> {
        self.check()?;
        self.node.campaign()?;
        Ok(())
    }
    pub fn tick(&mut self) -> Result<(), ControlError> {
        self.check()?;
        self.node.tick()?;
        Ok(())
    }
    /// Ticks between a leader's heartbeats.
    pub fn heartbeat_tick(&self) -> usize {
        self.node.heartbeat_tick()
    }
    /// A leader sends its heartbeats now; see `DurableNode::beat`.
    pub fn beat(&mut self) -> Result<(), ControlError> {
        self.check()?;
        self.node.beat()?;
        Ok(())
    }
    pub fn leads(&self) -> bool {
        self.node.status().role == focal_consensus::StateRole::Leader
    }
    /// Trusted in-process transport/testing seam. Production ingress must bind
    /// cluster/group and sender identity before delivering the message.
    pub fn step(&mut self, message: Message) -> Result<(), ControlError> {
        self.check()?;
        self.node.step(message)?;
        Ok(())
    }
    pub fn step_authenticated(
        &mut self,
        peer_node: u64,
        encoded: &[u8],
    ) -> Result<(), ControlError> {
        self.check()?;
        self.node.step_authenticated(peer_node, encoded)?;
        Ok(())
    }
    /// A read asked here waits for a round that leaves with the next drain
    /// (`DurableNode::reads_unasked`): an owner takes what else is queued
    /// for it first.
    pub fn reads_unasked(&self) -> bool {
        self.node.reads_unasked()
    }
    /// The reads asked here that wait for a quorum to confirm them.
    pub fn reads_waiting(&self) -> usize {
        self.node.reads_waiting()
    }
    /// Ask the group's leader for the index a read is served at: a leader
    /// confirms it itself, once ready; a follower asks through the leader
    /// it knows (the core forwards the read and the answer names the
    /// leader's commit; 27 §5, follower reads) and serves the read once it
    /// has applied that index. Before, a read was served by the leader
    /// alone, so every control read on a node whose root followed another
    /// voter — the founder's own startup asking for the membership, an
    /// operator's `membership show` — was refused `not_leader` (2026-10-02,
    /// the first root with three voters).
    pub fn read_index(&mut self, context: Vec<u8>) -> Result<(), ControlError> {
        self.check()?;
        // A replica whose held barriers are at their bound is too far
        // behind to take another read: refused here, typed, rather than
        // dropped when its answer comes.
        if self.parked_reads.len() >= self.node.pending_reads() {
            return Err(ControlError::Capacity);
        }
        let status = self.node.status();
        if status.role == StateRole::Leader {
            self.check_ready()?;
        } else {
            if status.leader_id == 0 {
                return Err(ControlError::Consensus(
                    focal_consensus::ConsensusError::NotLeader { leader: 0 },
                ));
            }
            if !self.drained {
                return Err(ControlError::NotReady);
            }
        }
        self.node.read_index(context)?;
        Ok(())
    }
    pub fn inject_fault_once(&mut self, fault: FaultPoint) -> Result<(), ControlError> {
        Ok(self.node.inject_fault_once(fault)?)
    }
    pub fn report_snapshot(
        &mut self,
        peer: u64,
        status: focal_consensus::SnapshotStatus,
    ) -> Result<(), ControlError> {
        self.check()?;
        self.node.report_snapshot(peer, status)?;
        Ok(())
    }
    /// A delayed transport completion applies only to the same leader term and
    /// exact pending snapshot index still owned by this replica.
    pub fn report_snapshot_at(
        &mut self,
        peer: u64,
        term: u64,
        index: u64,
        status: focal_consensus::SnapshotStatus,
    ) -> Result<(), ControlError> {
        self.check()?;
        self.node.report_snapshot_at(peer, term, index, status)?;
        Ok(())
    }
    /// Trusted transport feedback: an exchange with `peer` was lost; the
    /// core probes the member instead of streaming to it.
    pub fn report_unreachable(&mut self, peer: u64) -> Result<(), ControlError> {
        self.check()?;
        self.node.report_unreachable(peer)?;
        Ok(())
    }

    pub fn submit(
        &mut self,
        request: ControlRequest,
        verifier: &impl AuthorityVerifier,
    ) -> Result<ControlSubmission, ControlError> {
        self.check()?;
        if !self.drained {
            return Err(ControlError::NotReady);
        }
        let request_bytes = postcard::experimental::serialized_size(&request)?;
        if request_bytes > self.options.limits.max_command_bytes {
            return Err(ControlError::Capacity);
        }
        let request_hash = request.digest()?;
        if let Some(receipt) = self.retries.existing(&request, request_hash)? {
            return Ok(ControlSubmission::Existing(receipt));
        }
        if let Some(pending) = &self.pending {
            return if pending.request == request.id {
                if pending.request_hash == request_hash {
                    Ok(ControlSubmission::Pending(request.id))
                } else {
                    Err(ControlError::RetryConflict)
                }
            } else {
                Err(ControlError::Busy)
            };
        }
        self.check_ready()?;
        if let ControlCommand::Membership(change) = &request.command
            && (change.expected_configuration_index != self.configuration_index
                || change.expected != self.node.membership_configuration())
        {
            return Err(focal_directory::DirectoryError::CompareFailed.into());
        }
        let status = self.node.status();
        let envelope = CommandEnvelope {
            schema: COMMAND_SCHEMA,
            identity: self.identity,
            owner_node: status.node_id,
            owner_term: status.term,
            request,
        };
        // Serialize the envelope once: its bytes give the length (for admission
        // and the machine's charge), its hash, and the committed command, instead
        // of serializing it for each of those in turn.
        let encoded = encode(&envelope, self.options.limits.max_command_bytes)?;
        let encoded_len = encoded.len();
        let allocation = self
            .budget
            .reserve(
                BudgetKind::Pending,
                BudgetLane::Completion,
                charge(encoded_len.saturating_add(4096), 16)?,
            )?
            .commit();
        let retries = self.retries.prepare(
            &envelope.request,
            request_hash,
            &self.options.limits,
            &self.budget,
        )?;
        let machine = self.machine.prepare(
            &envelope.request.command,
            encoded_len,
            &self.budget,
            verifier,
            self.identity,
            &self.options,
        )?;
        let envelope_hash = hash_bytes("focal.control.envelope.v1", &encoded);
        let membership = if let ControlCommand::Membership(change) = &envelope.request.command {
            Some(PreparedConfiguration {
                index: change.expected_configuration_index,
                before: change.expected.clone(),
                after: change.change.apply_to(&change.expected)?,
            })
        } else {
            None
        };
        if let ControlCommand::Membership(change) = &envelope.request.command {
            // A group founded here on a sealed image compacts before it
            // admits its first member: the member holds no image to replay
            // the log's beginning onto, and once the floor stands past the
            // image the first thing the member is sent is a snapshot, by
            // construction — no tick's timing decides it (24 §13). A
            // compaction refused for the moment answers not ready; the
            // admission is asked again.
            if self.founded_from_image
                && self.node.snapshot_index() == 0
                && matches!(
                    change.change,
                    focal_consensus::MembershipChange::AddLearner { .. }
                )
            {
                // The checkpoint completes as it becomes durable; the floor
                // stands only then, so the admission is refused until it
                // does and asked again — never proposed on a log that still
                // holds the image's prefix.
                match self.checkpoint() {
                    Ok(()) => {}
                    Err(error) if self.checkpoint_retryable(&error) => {}
                    Err(error) => return Err(error),
                }
                return Err(ControlError::NotReady);
            }
            self.node
                .propose_membership(&change.expected, change.change, encoded)?;
        } else {
            self.node.propose(encoded)?;
        }
        let request = envelope.request.id;
        self.pending = Some(Pending {
            request,
            request_hash,
            envelope_hash,
            membership,
            term: status.term,
            machine,
            retries,
            _allocation: allocation,
        });
        Ok(ControlSubmission::Pending(request))
    }

    /// Persist Raft state before releasing messages; publish all committed
    /// metadata before emitting completion or fulfilled ReadIndex barriers.
    /// Any persistence/replay/publication failure fail-stops this owner.
    /// A drain the node refused before it took anything, for the room or
    /// for what it still persists, is none: the node is as it was, and the
    /// drain is asked again (`checkpoint_retryable`).
    pub fn drain(
        &mut self,
        verifier: &impl AuthorityVerifier,
    ) -> Result<ControlEvents, ControlError> {
        self.check()?;
        if let Some(delivery) = self.retained.take() {
            return self.drive(delivery, verifier);
        }
        let events = match self.node.drain() {
            Ok(events) => events,
            Err(error) if !self.node.failed() => return Err(error.into()),
            Err(error) => {
                self.fail(&error);
                return Err(error.into());
            }
        };
        self.drive(RetainedDelivery::new(events), verifier)
    }
    /// Deliver, or retain what a refusal that changed nothing of the node
    /// stopped — memory for an entry's decode or a snapshot's restore — for
    /// the next drain, which continues at the same entry; anything else
    /// fails the replica by its name. The owner treats the refusal as it
    /// treats a checkpoint's (`checkpoint_retryable`): the pace refused, the
    /// poll after resumes.
    fn drive(
        &mut self,
        mut delivery: RetainedDelivery,
        verifier: &impl AuthorityVerifier,
    ) -> Result<ControlEvents, ControlError> {
        match self.continue_delivery(&mut delivery, verifier) {
            Ok(()) => Ok(delivery.output),
            Err(error @ ControlError::Memory(_)) => {
                self.retained = Some(delivery);
                Err(error)
            }
            Err(error) => {
                self.fail(&error);
                Err(error)
            }
        }
    }
    /// The replica stops at `error`: what it delivered is not whole, and a
    /// reopen recovers it. The first cause is kept by name.
    fn fail(&mut self, error: &dyn std::fmt::Display) {
        self.failed = true;
        self.pending = None;
        if self.failure.is_none() {
            self.failure = Some(error.to_string());
        }
    }
    /// Why the replica failed, where it has.
    pub fn failure(&self) -> Option<&str> {
        self.failure.as_deref()
    }
    /// The drain without the wait for the disk: none while the node still
    /// persists what it took, when an owner sends what may be sent
    /// meanwhile (`sendable`) and waits for the write (`wait_persisted`).
    /// What it gives is what `drain` gives, whole.
    pub fn try_drain(
        &mut self,
        verifier: &impl AuthorityVerifier,
    ) -> Result<Option<ControlEvents>, ControlError> {
        self.check()?;
        if let Some(delivery) = self.retained.take() {
            return self.drive(delivery, verifier).map(Some);
        }
        let events = match self.node.try_drain() {
            Ok(Some(events)) => events,
            Ok(None) => return Ok(None),
            Err(error) if !self.node.failed() => return Err(error.into()),
            Err(error) => {
                self.fail(&error);
                return Err(error.into());
            }
        };
        self.drive(RetainedDelivery::new(events), verifier)
            .map(Some)
    }
    /// The Raft messages that may be sent while the node's write is in
    /// flight (`DurableNode::sendable`): a leader's, which its members
    /// persist for themselves.
    pub fn sendable(&mut self) -> Result<Option<focal_consensus::NodeEvents>, ControlError> {
        self.check()?;
        Ok(self.node.sendable()?)
    }
    /// Waits for the write the node has in flight, when it has one
    /// (`DurableNode::wait_persisted`).
    pub fn wait_persisted(&mut self) -> Result<bool, ControlError> {
        self.check()?;
        Ok(self.node.wait_persisted()?)
    }
    /// Deliver what the node handed over, from where the delivery stands:
    /// a delivery a refusal stopped is retained and continued at the same
    /// entry by the next drain, nothing applied twice (`RetainedDelivery`).
    fn continue_delivery(
        &mut self,
        delivery: &mut RetainedDelivery,
        verifier: &impl AuthorityVerifier,
    ) -> Result<(), ControlError> {
        if !delivery.snapshot_done
            && let Some(snapshot) = &delivery.events.snapshot
        {
            let _decode = self.budget.reserve(
                BudgetKind::Recovery,
                BudgetLane::Completion,
                charge(snapshot.data.len().saturating_add(8192), 32)?,
            )?;
            let (schema, _) = postcard::take_from_bytes::<u16>(&snapshot.data)?;
            let limit = self.options.limits.max_checkpoint_bytes;
            let (checkpoint, authority, configuration_index, contacts, membership) = match schema {
                1 => {
                    let legacy: Checkpoint<LegacyControlBootstrap> = decode(&snapshot.data, limit)?;
                    (
                        Checkpoint {
                            schema: CHECKPOINT_SCHEMA,
                            identity: legacy.identity,
                            applied_index: legacy.applied_index,
                            state: legacy.state.try_into()?,
                            retries: legacy.retries,
                        },
                        None,
                        0,
                        None,
                        None,
                    )
                }
                2 => {
                    let newer: CheckpointV2<LegacyControlBootstrap> =
                        decode(&snapshot.data, limit)?;
                    (
                        Checkpoint {
                            schema: CHECKPOINT_SCHEMA,
                            identity: newer.identity,
                            applied_index: newer.applied_index,
                            state: newer.state.try_into()?,
                            retries: newer.retries,
                        },
                        Some(newer.authority),
                        0,
                        None,
                        None,
                    )
                }
                3 => {
                    let newer: CheckpointV3<LegacyControlBootstrap> =
                        decode(&snapshot.data, limit)?;
                    (
                        Checkpoint {
                            schema: CHECKPOINT_SCHEMA,
                            identity: newer.identity,
                            applied_index: newer.applied_index,
                            state: newer.state.try_into()?,
                            retries: newer.retries,
                        },
                        newer.authority,
                        newer.configuration_index,
                        newer.contacts.map(ContactCheckpoint::from),
                        None,
                    )
                }
                4 => {
                    let newer: CheckpointV3 = decode(&snapshot.data, limit)?;
                    (
                        Checkpoint {
                            schema: CHECKPOINT_SCHEMA,
                            identity: newer.identity,
                            applied_index: newer.applied_index,
                            state: newer.state,
                            retries: newer.retries,
                        },
                        newer.authority,
                        newer.configuration_index,
                        newer.contacts.map(ContactCheckpoint::from),
                        None,
                    )
                }
                5 => {
                    let newer: CheckpointV5 = decode(&snapshot.data, limit)?;
                    (
                        Checkpoint {
                            schema: CHECKPOINT_SCHEMA,
                            identity: newer.identity,
                            applied_index: newer.applied_index,
                            state: newer.state,
                            retries: newer.retries,
                        },
                        newer.authority,
                        newer.configuration_index,
                        newer.contacts.map(ContactCheckpoint::from),
                        None,
                    )
                }
                6 => {
                    let newer: CheckpointV6 = decode(&snapshot.data, limit)?;
                    (
                        Checkpoint {
                            schema: CHECKPOINT_SCHEMA,
                            identity: newer.identity,
                            applied_index: newer.applied_index,
                            state: newer.state,
                            retries: newer.retries,
                        },
                        newer.authority,
                        newer.configuration_index,
                        newer.contacts.map(ContactCheckpoint::from),
                        None,
                    )
                }
                7 => {
                    let newer: CheckpointV7 = decode(&snapshot.data, limit)?;
                    (
                        Checkpoint {
                            schema: CHECKPOINT_SCHEMA,
                            identity: newer.identity,
                            applied_index: newer.applied_index,
                            state: newer.state,
                            retries: newer.retries,
                        },
                        newer.authority,
                        newer.configuration_index,
                        newer.contacts,
                        None,
                    )
                }
                8 => {
                    let newer: CheckpointV8 = decode(&snapshot.data, limit)?;
                    (
                        Checkpoint {
                            schema: CHECKPOINT_SCHEMA,
                            identity: newer.identity,
                            applied_index: newer.applied_index,
                            state: newer.state,
                            retries: newer.retries,
                        },
                        newer.authority,
                        newer.configuration_index,
                        newer.contacts,
                        newer.membership,
                    )
                }
                _ => return Err(ControlError::Corrupt("checkpoint schema")),
            };
            if checkpoint.schema != CHECKPOINT_SCHEMA
                || checkpoint.identity != self.identity
                || checkpoint.applied_index != snapshot.index
                || checkpoint.applied_index < self.applied_index
            {
                return Err(ControlError::Corrupt("checkpoint identity or prefix"));
            }
            if configuration_index > checkpoint.applied_index
                || configuration_index < self.configuration_index
            {
                return Err(ControlError::Corrupt("configuration index regression"));
            }
            self.validate_state_scope(&checkpoint.state)?;
            let retries = RetryState::restore(
                checkpoint.retries,
                &self.options.limits,
                &self.budget,
                checkpoint.applied_index,
            )?;
            let mut machine = Machine::restore(checkpoint.state, &self.options, &self.budget)?;
            if let Some(authority) = authority {
                machine.restore_authority(
                    authority,
                    self.identity,
                    checkpoint.applied_index,
                    &self.options,
                    &self.budget,
                )?;
            } else if self.machine.authority().is_some() {
                return Err(ControlError::Corrupt(
                    "authority activation cannot disappear",
                ));
            }
            if let Some(contacts) = contacts {
                machine.restore_contacts(
                    contacts,
                    &self.options,
                    &self.budget,
                    checkpoint.applied_index,
                )?;
            } else if self.machine.contacts().is_some() {
                return Err(ControlError::Corrupt("contacts cannot disappear"));
            }
            self.configuration_index = configuration_index;
            if membership
                .as_ref()
                .is_some_and(|record| record.index != configuration_index)
            {
                return Err(ControlError::Corrupt("checkpoint membership record"));
            }
            self.membership = membership;
            if let Some(pending) = self.pending.take() {
                delivery.output.uncertain = Some(pending.request);
            }
            self.machine = machine;
            self.retries = retries;
            self.applied_index = checkpoint.applied_index;
            delivery.snapshot_done = true;
        }
        // A member of a group founded on an image it does not hold applies
        // no entry before a snapshot: the founder compacted at founding, so
        // an entry from the log's beginning reaching an empty replica is a
        // law broken, not a state to build on (its state would diverge from
        // the group's at the first command).
        if self.options.founded_elsewhere.is_some()
            && self.applied_index == 0
            && (delivery
                .events
                .committed
                .first()
                .is_some_and(|entry| entry.index == 1)
                || delivery
                    .events
                    .membership
                    .first()
                    .is_some_and(|change| change.index == 1))
        {
            return Err(ControlError::Corrupt(
                "a member founded elsewhere applies no entry before a snapshot",
            ));
        }
        // The entries and the configuration changes, merged by index, from
        // the delivery's cursors: an entry is read in place and its cursor
        // moved past it once it is applied, so a delivery continued after a
        // refusal begins at the entry the refusal stopped.
        loop {
            let next_entry = delivery
                .events
                .committed
                .get(delivery.entry)
                .map(|entry| entry.index);
            let next_change = delivery
                .events
                .membership
                .get(delivery.configuration)
                .map(|change| change.index);
            let configuration_first = match (next_entry, next_change) {
                (Some(entry), Some(change)) => change < entry,
                (None, Some(_)) => true,
                (Some(_), None) => false,
                (None, None) => break,
            };
            let (entry, membership) = if configuration_first {
                let change = delivery
                    .events
                    .membership
                    .get(delivery.configuration)
                    .ok_or(ControlError::Corrupt("configuration vanished"))?;
                (
                    DeliveredEntry {
                        index: change.index,
                        term: change.term,
                        data: &change.context,
                    },
                    Some(change),
                )
            } else {
                let entry = delivery
                    .events
                    .committed
                    .get(delivery.entry)
                    .ok_or(ControlError::Corrupt("command vanished"))?;
                (
                    DeliveredEntry {
                        index: entry.index,
                        term: entry.term,
                        data: &entry.data,
                    },
                    None,
                )
            };
            'entry: {
                if entry.index <= self.applied_index {
                    return Err(ControlError::Corrupt("application index regression"));
                }
                let prior_configuration = self.configuration_index;
                if let Some(applied) = &membership {
                    self.configuration_index = entry.index;
                    if entry.data.is_empty() {
                        self.membership = Some(ControlMembershipRecord::of_entry(
                            entry.index,
                            entry.term,
                            applied.after.clone(),
                        )?);
                        self.applied_index = entry.index;
                        break 'entry;
                    }
                }
                let encoded_hash = blake3::derive_key("focal.control.envelope.v1", entry.data);
                if self
                    .pending
                    .as_ref()
                    .is_some_and(|pending| pending.envelope_hash == encoded_hash)
                {
                    // Exact locally admitted bytes already own their complete next
                    // state and receipt. Publication needs no decode or reservation.
                    let pending = self
                        .pending
                        .take()
                        .ok_or(ControlError::Corrupt("pending vanished"))?;
                    if pending.term != entry.term {
                        return Err(ControlError::WrongOwner);
                    }
                    match (&pending.membership, &membership) {
                        (None, None) => {}
                        (Some(expected), Some(applied))
                            if expected.index == prior_configuration
                                && expected.before == applied.before
                                && expected.after == applied.after => {}
                        _ => return Err(ControlError::Corrupt("prepared membership result")),
                    }
                    self.machine.publish(pending.machine, entry.index)?;
                    let mut retries = pending.retries;
                    let receipt = retries.complete(
                        pending.request,
                        entry.index,
                        entry.term,
                        self.machine.revisions(),
                    )?;
                    self.retries = retries;
                    if let Some(applied) = &membership {
                        self.membership = Some(ControlMembershipRecord {
                            index: entry.index,
                            term: entry.term,
                            request_hash: pending.request_hash,
                            configuration: applied.after.clone(),
                        });
                    }
                    self.applied_index = entry.index;
                    delivery.output.completed = Some(receipt);
                    break 'entry;
                }
                let _decode = self.budget.reserve(
                    BudgetKind::Recovery,
                    BudgetLane::Completion,
                    charge(entry.data.len().saturating_add(8192), 32)?,
                )?;
                let envelope: CommandEnvelope =
                    decode(entry.data, self.options.limits.max_command_bytes)?;
                // Schema 1 entries predate disk load reports and plan observations;
                // their layout is otherwise identical and decodes above.
                if !(1..=COMMAND_SCHEMA).contains(&envelope.schema)
                    || envelope.identity != self.identity
                    || envelope.owner_node == 0
                    || envelope.owner_term != entry.term
                {
                    return Err(ControlError::WrongOwner);
                }
                match (&envelope.request.command, membership.as_ref()) {
                    (ControlCommand::Membership(command), Some(applied)) => {
                        if command.expected_configuration_index != prior_configuration
                            || command.expected != applied.before
                            || command.change.apply_to(&applied.before)? != applied.after
                        {
                            return Err(ControlError::Corrupt("committed membership precondition"));
                        }
                    }
                    (ControlCommand::Membership(_), None) | (_, Some(_)) => {
                        return Err(ControlError::Corrupt("command entry type"));
                    }
                    _ => {}
                }
                let request_hash = envelope.request.digest()?;
                if let Some(applied) = &membership {
                    self.membership = Some(ControlMembershipRecord {
                        index: entry.index,
                        term: entry.term,
                        request_hash,
                        configuration: applied.after.clone(),
                    });
                }
                if let Some(existing) = self.retries.existing(&envelope.request, request_hash)? {
                    if self
                        .pending
                        .as_ref()
                        .is_some_and(|pending| pending.request == existing.request)
                    {
                        self.pending = None;
                        delivery.output.completed = Some(existing);
                    }
                    self.applied_index = entry.index;
                    break 'entry;
                }
                if let Some(pending) = self.pending.take() {
                    delivery.output.uncertain = Some(pending.request);
                }
                let mut prepared_retries = self.retries.prepare(
                    &envelope.request,
                    request_hash,
                    &self.options.limits,
                    &self.budget,
                )?;
                let prepared_machine = self.machine.prepare(
                    &envelope.request.command,
                    entry.data.len(),
                    &self.budget,
                    verifier,
                    self.identity,
                    &self.options,
                )?;
                self.machine.publish(prepared_machine, entry.index)?;
                prepared_retries.complete(
                    envelope.request.id,
                    entry.index,
                    entry.term,
                    self.machine.revisions(),
                )?;
                self.retries = prepared_retries;
                self.applied_index = entry.index;
            }
            if configuration_first {
                delivery.configuration = delivery
                    .configuration
                    .checked_add(1)
                    .ok_or(ControlError::Capacity)?;
            } else {
                delivery.entry = delivery
                    .entry
                    .checked_add(1)
                    .ok_or(ControlError::Capacity)?;
            }
        }
        if delivery.events.applied_index < self.applied_index {
            return Err(ControlError::Corrupt("Raft prefix regression"));
        }
        self.applied_index = delivery.events.applied_index;
        let status = self.node.status();
        if self
            .pending
            .as_ref()
            .is_some_and(|pending| pending.term != status.term || status.role != StateRole::Leader)
            && let Some(pending) = self.pending.take()
        {
            delivery.output.uncertain = Some(pending.request);
        }
        // A barrier answered above what this replica has applied is a
        // follower's read answered with its leader's commit (27 §5, the
        // session's rule since F55): held until the entries it names are
        // applied, never corruption. It was taken for corruption, and a
        // member brought up by snapshot that read before it caught up
        // failed (`a_crowded_partition_splits_survives_a_restart_and_merges_back`,
        // ubuntu CI and a local run, 2026-10-03). Those held before that this
        // delivery reached go out with the rest.
        self.park_ahead(&mut delivery.events.read_states)?;
        let released = self.release_parked(&mut delivery.events.read_states)?;
        delivery.output.messages = std::mem::take(&mut delivery.events.messages);
        delivery.output.read_states = std::mem::take(&mut delivery.events.read_states);
        delivery.output.parked = released;
        delivery.output.applied_index = self.applied_index;
        self.drained = true;
        Ok(())
    }
    /// Hold the barriers of `states` answered above the applied index. What
    /// they take is reserved before any is moved, so a refusal for memory
    /// leaves the delivery as it was, to be continued; beyond the bound —
    /// the reads the core holds in flight — a barrier is dropped and
    /// counted, and the replica refuses new reads until its held ones go
    /// (`read_index`).
    fn park_ahead(&mut self, states: &mut Vec<ReadBarrier>) -> Result<(), ControlError> {
        let bound = self.node.pending_reads();
        let room = bound.saturating_sub(self.parked_reads.len());
        let mut bytes = 0usize;
        let mut count = 0usize;
        for read in states
            .iter()
            .filter(|read| read.index > self.applied_index)
            .take(room)
        {
            bytes = bytes
                .checked_add(parked_bytes(read)?)
                .ok_or(ControlError::Capacity)?;
            count = count.checked_add(1).ok_or(ControlError::Capacity)?;
        }
        if count > 0 {
            self.parked_reads
                .try_reserve(count)
                .map_err(|_| ControlError::Capacity)?;
            let mut charge = self
                .budget
                .reserve(BudgetKind::Pending, BudgetLane::Completion, bytes)?
                .commit();
            match &mut self.parked_charge {
                Some(held) => held
                    .absorb(&mut charge)
                    .map_err(|_| ControlError::Capacity)?,
                None => self.parked_charge = Some(charge),
            }
        }
        let mut index = 0;
        while let Some(read) = states.get(index) {
            if read.index <= self.applied_index {
                index = index.checked_add(1).ok_or(ControlError::Capacity)?;
                continue;
            }
            let barrier = states.remove(index);
            if self.parked_reads.len() < bound {
                self.parked_reads.push(barrier);
                self.reads_parked = self.reads_parked.saturating_add(1);
            } else {
                self.reads_dropped = self.reads_dropped.saturating_add(1);
            }
        }
        Ok(())
    }
    /// Move the held barriers the applied index has reached into `states`,
    /// their charge with them.
    fn release_parked(
        &mut self,
        states: &mut Vec<ReadBarrier>,
    ) -> Result<Option<Allocation>, ControlError> {
        let reached = self
            .parked_reads
            .iter()
            .filter(|read| read.index <= self.applied_index)
            .count();
        if reached == 0 {
            return Ok(None);
        }
        states
            .try_reserve(reached)
            .map_err(|_| ControlError::Capacity)?;
        let mut released: Option<Allocation> = None;
        let mut index = 0;
        while let Some(read) = self.parked_reads.get(index) {
            if read.index > self.applied_index {
                index = index.checked_add(1).ok_or(ControlError::Capacity)?;
                continue;
            }
            let bytes = parked_bytes(read)?;
            let mut part = self
                .parked_charge
                .as_mut()
                .ok_or(ControlError::Corrupt(
                    "held read barrier without its charge",
                ))?
                .split_off(bytes)
                .map_err(|_| ControlError::Corrupt("held read barrier without its charge"))?;
            match &mut released {
                Some(all) => all.absorb(&mut part).map_err(|_| ControlError::Capacity)?,
                None => released = Some(part),
            }
            states.push(self.parked_reads.remove(index));
        }
        Ok(released)
    }
    /// Barriers held above the applied index, and dropped at the bound,
    /// since open (27 §5).
    pub fn reads_parked(&self) -> u64 {
        self.reads_parked
    }
    pub fn reads_dropped(&self) -> u64 {
        self.reads_dropped
    }
    pub fn checkpoint(&mut self) -> Result<(), ControlError> {
        self.check()?;
        if !self.drained || self.applied_index == 0 {
            return Err(ControlError::NotReady);
        }
        if self.pending.is_some() {
            return Err(ControlError::Busy);
        }
        let estimate = self
            .machine
            .export_estimate()?
            .checked_add(self.machine.authority_estimate()?)
            .and_then(|n| n.checked_add(self.machine.contact_charge()))
            .ok_or(ControlError::Capacity)?
            .checked_add(postcard::experimental::serialized_size(
                &self.retries.checkpoint,
            )?)
            .and_then(|n| n.checked_add(8192))
            .ok_or(ControlError::Capacity)?;
        let _scratch = self.budget.reserve(
            BudgetKind::Recovery,
            BudgetLane::Completion,
            charge(estimate, 32)?,
        )?;
        let authority = self
            .machine
            .export_authority(self.identity, self.applied_index)?;
        let bytes = encode(
            &CheckpointV8 {
                schema: CHECKPOINT_SCHEMA,
                identity: self.identity,
                applied_index: self.applied_index,
                state: self.machine.export()?,
                retries: self.retries.checkpoint.clone(),
                authority,
                configuration_index: self.configuration_index,
                contacts: self.machine.contacts().cloned(),
                membership: self.membership.clone(),
            },
            self.options.limits.max_checkpoint_bytes,
        )?;
        let result = self.node.checkpoint(self.applied_index, bytes);
        if result.is_err() {
            // A checkpoint the log had no room to admit stays unadmitted:
            // withdraw it so the node is mutable again and a later attempt
            // starts afresh. Nothing was written.
            self.node.cancel_unadmitted_checkpoint();
            // Only a node that actually stopped stops this replica. A refusal
            // that changed nothing (Raft has work outstanding, persistence is
            // in flight, memory or log pressure) is retried by the caller;
            // treating it as fatal ended the root leader under ordinary load.
            if self.node.failed()
                && let Err(error) = &result
            {
                self.fail(error);
            }
        }
        result.map_err(ControlError::from)
    }
    /// Whether a checkpoint error is a refusal that changed nothing and may
    /// be retried later, rather than a failure of this replica.
    pub fn checkpoint_retryable(&self, error: &ControlError) -> bool {
        !self.failed
            && matches!(
                error,
                ControlError::NotReady
                    | ControlError::Busy
                    | ControlError::Memory(_)
                    | ControlError::Consensus(
                        focal_consensus::ConsensusError::PersistencePending
                            | focal_consensus::ConsensusError::CheckpointIndex
                            | focal_consensus::ConsensusError::Capacity
                    )
            )
    }
    fn validate_state_scope(&self, state: &ControlBootstrap) -> Result<(), ControlError> {
        match (self.identity.scope, state) {
            (ControlScope::Root, ControlBootstrap::Root { directory, .. })
                if directory.cluster == self.identity.cluster =>
            {
                Ok(())
            }
            (ControlScope::Partition(id), ControlBootstrap::Partition { directory })
                if directory.cluster == self.identity.cluster
                    && directory.delegation.partition == id
                    && directory.delegation.log_group.0 == self.identity.group =>
            {
                Ok(())
            }
            _ => Err(ControlError::WrongOwner),
        }
    }
    fn check(&self) -> Result<(), ControlError> {
        if self.failed {
            Err(ControlError::Failed)
        } else {
            Ok(())
        }
    }
    fn check_ready(&self) -> Result<(), ControlError> {
        self.check()?;
        let status = self.node.status();
        if status.role != StateRole::Leader {
            return Err(ControlError::Consensus(
                focal_consensus::ConsensusError::NotLeader {
                    leader: status.leader_id,
                },
            ));
        }
        if !self.drained
            || !self.node.has_committed_current_term()
            || self.applied_index != status.committed_index
            || self.applied_index != status.applied_index
        {
            return Err(ControlError::NotReady);
        }
        Ok(())
    }
}

/// What a held read barrier is charged: the barrier and its context.
fn parked_bytes(read: &ReadBarrier) -> Result<usize, ControlError> {
    size_of::<ReadBarrier>()
        .checked_add(read.context.capacity())
        .ok_or(ControlError::Capacity)
}
