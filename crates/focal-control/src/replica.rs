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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Checkpoint {
    schema: u16,
    identity: ControlIdentity,
    applied_index: u64,
    state: ControlBootstrap,
    retries: RetryCheckpoint,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CheckpointV2 {
    schema: u16,
    identity: ControlIdentity,
    applied_index: u64,
    state: ControlBootstrap,
    retries: RetryCheckpoint,
    authority: ControlAuthoritySnapshot,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CheckpointV3 {
    schema: u16,
    identity: ControlIdentity,
    applied_index: u64,
    state: ControlBootstrap,
    retries: RetryCheckpoint,
    authority: Option<ControlAuthoritySnapshot>,
    configuration_index: u64,
    contacts: Option<ContactCheckpoint>,
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
    drained: bool,
    failed: bool,
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
        let machine = Machine::restore(bootstrap, &options, &budget)?;
        let retries = RetryState::restore(BTreeMap::new(), &options.limits, &budget, 0)?;
        let node = DurableNode::open_in(options.consensus.clone(), directory, &budget)?;
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
            drained: false,
            failed: false,
        })
    }
    pub fn open_on_wal(
        options: ControlOptions,
        bootstrap: ControlBootstrap,
        budget: MemoryBudget,
        wal: SharedWal,
    ) -> Result<Self, ControlError> {
        options.validate()?;
        let identity = bootstrap.identity(&options)?;
        let machine = Machine::restore(bootstrap, &options, &budget)?;
        let retries = RetryState::restore(BTreeMap::new(), &options.limits, &budget, 0)?;
        let node = DurableNode::open_on_wal_in(options.consensus.clone(), wal, &budget)?;
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
            drained: false,
            failed: false,
        })
    }
    pub fn identity(&self) -> ControlIdentity {
        self.identity
    }
    pub fn status(&self) -> NodeStatus {
        self.node.status()
    }
    pub fn applied_index(&self) -> u64 {
        self.applied_index
    }
    pub fn has_pending(&self) -> bool {
        self.pending.is_some()
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
            ControlRead::Configuration => 2048 * 16 + 4096,
            ControlRead::Contacts => self.machine.contact_charge(),
        };
        charge(bytes.checked_add(4096).ok_or(ControlError::Capacity)?, 4)
    }
    /// Local committed data only. The host must complete a matching ReadIndex
    /// first and retain its read allocation through encoding/delivery.
    pub fn read_local(&self, query: &ControlRead) -> Result<ControlReadResult, ControlError> {
        self.check()?;
        if !self.drained {
            return Err(ControlError::NotReady);
        }
        match query {
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
                            schema: 1,
                            cluster: self.identity.cluster.0,
                            revision: 0,
                            applied_index: 0,
                            records: Vec::new(),
                        }),
                }))
            }
            ControlRead::Contacts => Err(ControlError::WrongOwner),
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
    pub fn accepts_peer(&self, node: u64) -> bool {
        self.node.membership_configuration().contains(node)
    }
    pub fn transfer(&mut self, request: &ControlTransfer) -> Result<(), ControlError> {
        self.check_ready()?;
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
    pub fn receipt(&self, id: ControlRequestId) -> Result<Option<ControlReceipt>, ControlError> {
        self.check()?;
        if !self.drained {
            return Err(ControlError::NotReady);
        }
        self.retries.lookup(id)
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
    pub fn read_index(&mut self, context: Vec<u8>) -> Result<(), ControlError> {
        self.check_ready()?;
        self.node.read_index(context)?;
        Ok(())
    }
    pub fn inject_fault_once(&mut self, fault: FaultPoint) {
        self.node.inject_fault_once(fault);
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
            schema: 1,
            identity: self.identity,
            owner_node: status.node_id,
            owner_term: status.term,
            request,
        };
        let encoded_len = postcard::experimental::serialized_size(&envelope)?;
        if encoded_len > self.options.limits.max_command_bytes {
            return Err(ControlError::Capacity);
        }
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
        let envelope_hash = hash("focal.control.envelope.v1", &envelope)?;
        let encoded = encode(&envelope, self.options.limits.max_command_bytes)?;
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
    pub fn drain(
        &mut self,
        verifier: &impl AuthorityVerifier,
    ) -> Result<ControlEvents, ControlError> {
        self.check()?;
        let result = self.drain_inner(verifier);
        if result.is_err() {
            self.failed = true;
            self.pending = None;
        }
        result
    }
    fn drain_inner(
        &mut self,
        verifier: &impl AuthorityVerifier,
    ) -> Result<ControlEvents, ControlError> {
        let mut events = self.node.drain()?;
        let mut output = ControlEvents {
            allocation: events.take_allocation(),
            ..Default::default()
        };
        if let Some(snapshot) = events.snapshot {
            let _decode = self.budget.reserve(
                BudgetKind::Recovery,
                BudgetLane::Completion,
                charge(snapshot.data.len().saturating_add(8192), 32)?,
            )?;
            let (schema, _) = postcard::take_from_bytes::<u16>(&snapshot.data)?;
            let (checkpoint, authority, configuration_index, contacts) = match schema {
                1 => (
                    decode::<Checkpoint>(&snapshot.data, self.options.limits.max_checkpoint_bytes)?,
                    None,
                    0,
                    None,
                ),
                2 => {
                    let newer: CheckpointV2 =
                        decode(&snapshot.data, self.options.limits.max_checkpoint_bytes)?;
                    (
                        Checkpoint {
                            schema: 1,
                            identity: newer.identity,
                            applied_index: newer.applied_index,
                            state: newer.state,
                            retries: newer.retries,
                        },
                        Some(newer.authority),
                        0,
                        None,
                    )
                }
                3 => {
                    let newer: CheckpointV3 =
                        decode(&snapshot.data, self.options.limits.max_checkpoint_bytes)?;
                    (
                        Checkpoint {
                            schema: 1,
                            identity: newer.identity,
                            applied_index: newer.applied_index,
                            state: newer.state,
                            retries: newer.retries,
                        },
                        newer.authority,
                        newer.configuration_index,
                        newer.contacts,
                    )
                }
                _ => return Err(ControlError::Corrupt("checkpoint schema")),
            };
            if checkpoint.schema != 1
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
            if let Some(pending) = self.pending.take() {
                output.uncertain = Some(pending.request);
            }
            self.machine = machine;
            self.retries = retries;
            self.applied_index = checkpoint.applied_index;
        }
        let mut ordinary = events.committed.into_iter().peekable();
        let mut configurations = events.membership.into_iter().peekable();
        while ordinary.peek().is_some() || configurations.peek().is_some() {
            let configuration_first = match (ordinary.peek(), configurations.peek()) {
                (Some(entry), Some(change)) => change.index < entry.index,
                (None, Some(_)) => true,
                _ => false,
            };
            let (entry, membership) = if configuration_first {
                let mut change = configurations
                    .next()
                    .ok_or(ControlError::Corrupt("configuration vanished"))?;
                let entry = focal_consensus::CommittedEntry {
                    index: change.index,
                    term: change.term,
                    data: std::mem::take(&mut change.context),
                };
                (entry, Some(change))
            } else {
                (
                    ordinary
                        .next()
                        .ok_or(ControlError::Corrupt("command vanished"))?,
                    None,
                )
            };
            if entry.index <= self.applied_index {
                return Err(ControlError::Corrupt("application index regression"));
            }
            let prior_configuration = self.configuration_index;
            if membership.is_some() {
                self.configuration_index = entry.index;
                if entry.data.is_empty() {
                    self.applied_index = entry.index;
                    continue;
                }
            }
            let encoded_hash = blake3::derive_key("focal.control.envelope.v1", &entry.data);
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
                self.applied_index = entry.index;
                output.completed = Some(receipt);
                continue;
            }
            let _decode = self.budget.reserve(
                BudgetKind::Recovery,
                BudgetLane::Completion,
                charge(entry.data.len().saturating_add(8192), 32)?,
            )?;
            let envelope: CommandEnvelope =
                decode(&entry.data, self.options.limits.max_command_bytes)?;
            if envelope.schema != 1
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
            if let Some(existing) = self.retries.existing(&envelope.request, request_hash)? {
                if self
                    .pending
                    .as_ref()
                    .is_some_and(|pending| pending.request == existing.request)
                {
                    self.pending = None;
                    output.completed = Some(existing);
                }
                self.applied_index = entry.index;
                continue;
            }
            if let Some(pending) = self.pending.take() {
                output.uncertain = Some(pending.request);
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
        if events.applied_index < self.applied_index {
            return Err(ControlError::Corrupt("Raft prefix regression"));
        }
        self.applied_index = events.applied_index;
        let status = self.node.status();
        if self
            .pending
            .as_ref()
            .is_some_and(|pending| pending.term != status.term || status.role != StateRole::Leader)
            && let Some(pending) = self.pending.take()
        {
            output.uncertain = Some(pending.request);
        }
        if events
            .read_states
            .iter()
            .any(|read| read.index > self.applied_index)
        {
            return Err(ControlError::Corrupt("read barrier ahead of publication"));
        }
        output.messages = events.messages;
        output.read_states = events.read_states;
        output.applied_index = self.applied_index;
        self.drained = true;
        Ok(output)
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
        let checkpoint = Checkpoint {
            schema: 1,
            identity: self.identity,
            applied_index: self.applied_index,
            state: self.machine.export()?,
            retries: self.retries.checkpoint.clone(),
        };
        let authority = self
            .machine
            .export_authority(self.identity, self.applied_index)?;
        let bytes = if self.configuration_index > 0 || self.machine.contacts().is_some() {
            encode(
                &CheckpointV3 {
                    schema: 3,
                    identity: checkpoint.identity,
                    applied_index: checkpoint.applied_index,
                    state: checkpoint.state,
                    retries: checkpoint.retries,
                    authority,
                    configuration_index: self.configuration_index,
                    contacts: self.machine.contacts().cloned(),
                },
                self.options.limits.max_checkpoint_bytes,
            )?
        } else {
            match authority {
                None => encode(&checkpoint, self.options.limits.max_checkpoint_bytes)?,
                Some(authority) => encode(
                    &CheckpointV2 {
                        schema: 2,
                        identity: checkpoint.identity,
                        applied_index: checkpoint.applied_index,
                        state: checkpoint.state,
                        retries: checkpoint.retries,
                        authority,
                    },
                    self.options.limits.max_checkpoint_bytes,
                )?,
            }
        };
        let result = self.node.checkpoint(self.applied_index, bytes);
        if result.is_err() {
            self.failed = true;
        }
        result.map_err(ControlError::from)
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
