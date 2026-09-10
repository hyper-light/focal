pub use focal_directory::{LogGroupId, OperationId, PlacementSpec, SessionFence, SessionFenceKind};

const PLACEMENT_MAGIC: &[u8] = b"FOCALPL1";
const SNAPSHOT_V4_MAGIC: &[u8] = b"FOCALSS4";
const MAX_PLACEMENT_BYTES: usize = 64 * 1024;
const PLACEMENT_WORKSPACE: usize = 2 * 1024 * 1024;

/// Trusted session-owner intent. Coordinates of the resulting fence are assigned
/// by committed application, never accepted from this request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionPlacementRequest {
    pub expected_index: u64,
    pub expected_configuration_index: u64,
    pub operation: OperationId,
    pub kind: SessionFenceKind,
    pub from_route: RouteEpoch,
    pub to_route: RouteEpoch,
    pub membership_epoch: u64,
    pub placement_epoch: u64,
    pub placement: PlacementSpec,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PlacementRecord {
    schema: u16,
    ledger: LedgerId,
    group: LogGroupId,
    genesis: ContentHash,
    sequence: SessionSeq,
    configuration: MembershipConfiguration,
    request: SessionPlacementRequest,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StoredPlacement {
    record: PlacementRecord,
    fence: SessionFence,
}
#[derive(Default, Serialize, Deserialize)]
struct PlacementState {
    active: Option<StoredPlacement>,
    /// Retain the last cutover after activation for exact interrupted retries.
    cutover: Option<StoredPlacement>,
}
#[derive(Serialize, Deserialize)]
struct SnapshotEnvelopeV4 {
    state: SnapshotEnvelopeV3,
    placement: PlacementState,
}
struct PendingPlacementRecord {
    request: SessionPlacementRequest,
    _allocation: Allocation,
}
/// An owned local durable fact. No public constructor or deserializer can mint
/// this witness; only Session application/recovery and a bounded export can.
pub struct CommittedPlacement {
    stored: StoredPlacement,
    node: u64,
    cluster: [u8; 16],
    _allocation: Allocation,
}
impl CommittedPlacement {
    pub fn fence(&self) -> &SessionFence {
        &self.stored.fence
    }
    pub fn configuration(&self) -> &MembershipConfiguration {
        &self.stored.record.configuration
    }
    pub fn placement(&self) -> &PlacementSpec {
        &self.stored.record.request.placement
    }
    pub fn genesis(&self) -> ContentHash {
        self.stored.record.genesis
    }
    pub fn node(&self) -> u64 {
        self.node
    }
    pub fn cluster(&self) -> [u8; 16] {
        self.cluster
    }
}
impl PlacementState {
    fn latest(&self) -> Option<&StoredPlacement> {
        match (&self.active, &self.cutover) {
            (Some(active), Some(cutover)) if cutover.fence.index > active.fence.index => {
                Some(cutover)
            }
            (Some(active), _) => Some(active),
            (None, cutover) => cutover.as_ref(),
        }
    }
    fn paused(&self) -> bool {
        self.latest()
            .is_some_and(|record| record.fence.kind == SessionFenceKind::Cutover)
    }
    fn receipt(
        &self,
        request: &SessionPlacementRequest,
    ) -> Result<Option<&StoredPlacement>, LedgerError> {
        for record in [&self.active, &self.cutover].into_iter().flatten() {
            if record.record.request.operation == request.operation
                && record.record.request.kind == request.kind
            {
                if record.record.request != *request {
                    return Err(LedgerError::PlacementConflict);
                }
                return Ok(Some(record));
            }
        }
        Ok(None)
    }
}
impl SessionPlacementRequest {
    pub fn validate(&self) -> Result<(), LedgerError> {
        let placement = &self.placement.placement;
        match self.kind {
            SessionFenceKind::Created
                if self.expected_index != 0
                    || self.from_route != RouteEpoch(0)
                    || self.to_route != RouteEpoch(1)
                    || self.membership_epoch != 1
                    || self.placement_epoch != 1 =>
            {
                return Err(LedgerError::PlacementConflict);
            }
            SessionFenceKind::Cutover | SessionFenceKind::Activated
                if self.expected_index == 0
                    || self.from_route == RouteEpoch(0)
                    || self.placement_epoch < 2 =>
            {
                return Err(LedgerError::PlacementConflict);
            }
            _ => (),
        }
        if self.operation.0 == [0; 16]
            || self.membership_epoch == 0
            || self.placement_epoch == 0
            || self.from_route.0.checked_add(1) != Some(self.to_route.0)
            || placement.voters.is_empty()
            || !placement.voters.contains_key(&placement.preferred_leader)
            || self.placement.policy.residency.len() > 127
            || self.placement.policy.home_regions.len() > 127
        {
            return Err(LedgerError::PlacementConflict);
        }
        for members in [
            &placement.voters,
            &placement.materializers,
            &placement.content_copies,
        ] {
            if members.is_empty()
                || members.len() > 127
                || members
                    .iter()
                    .any(|(node, generation)| *node == 0 || *generation == 0)
            {
                return Err(LedgerError::PlacementConflict);
            }
        }
        for (node, generation) in placement
            .voters
            .iter()
            .chain(&placement.materializers)
            .chain(&placement.content_copies)
        {
            if [
                &placement.voters,
                &placement.materializers,
                &placement.content_copies,
            ]
            .into_iter()
            .any(|members| members.get(node).is_some_and(|other| other != generation))
            {
                return Err(LedgerError::PlacementConflict);
            }
        }
        if postcard::experimental::serialized_size(&focal_model::durable_v1::Ref(self))? > MAX_PLACEMENT_BYTES {
            return Err(LedgerError::Capacity);
        }
        Ok(())
    }
}
impl Session {
    /// Canonical immutable group identity, including the original durable voter
    /// and learner sets. Local node ID and operational tuning are excluded.
    pub fn placement_genesis(&self) -> Result<ContentHash, LedgerError> {
        let mut hash = blake3::Hasher::new_derive_key("focal.session.placement-genesis.v1");
        hash.update(&self.cluster_id());
        hash.update(&self.ledger.tenant.0);
        hash.update(&self.ledger.session.0);
        hash.update(&self.group_id());
        let (voters, learners) = self.consensus.bootstrap_membership();
        for members in [voters, learners] {
            hash.update(
                &u64::try_from(members.len())
                    .map_err(|_| LedgerError::Capacity)?
                    .to_be_bytes(),
            );
            let mut previous = None;
            // Bootstrap sets are bounded at 1024 and may have noncanonical input
            // order. Stream canonical IDs without allocating a sorting buffer.
            for _ in 0..members.len() {
                let next = members
                    .iter()
                    .filter(|id| previous.is_none_or(|old| **id > old))
                    .min()
                    .copied()
                    .ok_or(LedgerError::Corrupt)?;
                hash.update(&next.to_be_bytes());
                previous = Some(next);
            }
        }
        Ok(ContentHash(*hash.finalize().as_bytes()))
    }
    pub fn placement(&self) -> Option<SessionFence> {
        self.placement_state
            .latest()
            .map(|record| record.fence.clone())
    }
    pub fn active_route(&self) -> Option<RouteEpoch> {
        self.placement_state
            .active
            .as_ref()
            .map(|record| record.fence.to_route)
    }
    /// The fence of the active record, as opposed to a later cutover record
    /// whose activation is still pending.
    pub fn active_fence(&self) -> Option<&SessionFence> {
        self.placement_state
            .active
            .as_ref()
            .map(|record| &record.fence)
    }
    /// The placement the active record committed, with its members.
    pub fn active_placement(&self) -> Option<&PlacementSpec> {
        self.placement_state
            .active
            .as_ref()
            .map(|record| &record.record.request.placement)
    }
    pub fn placement_receipt(
        &self,
        request: &SessionPlacementRequest,
    ) -> Result<Option<SessionFence>, LedgerError> {
        self.check()?;
        request.validate()?;
        Ok(self
            .placement_state
            .receipt(request)?
            .map(|record| record.fence.clone()))
    }
    pub fn placement_witness(
        &self,
        request: &SessionPlacementRequest,
    ) -> Result<Option<CommittedPlacement>, LedgerError> {
        self.check()?;
        request.validate()?;
        if self.persistence_pending() {
            return Err(ConsensusError::PersistencePending.into());
        }
        let Some(stored) = self.placement_state.receipt(request)? else {
            return Ok(None);
        };
        let allocation = self
            .budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                placement_charge(stored)?,
            )?
            .commit();
        Ok(Some(CommittedPlacement {
            stored: stored.clone(),
            node: self.status().node_id,
            cluster: self.cluster_id(),
            _allocation: allocation,
        }))
    }
    pub fn propose_placement(
        &mut self,
        request: &SessionPlacementRequest,
    ) -> Result<(), LedgerError> {
        self.check()?;
        request.validate()?;
        if !self.is_authoritative() {
            return Err(LedgerError::NotReady {
                leader: self.status().leader_id,
            });
        }
        if self.placement_state.receipt(request)?.is_some() {
            return Ok(());
        }
        if let Some(pending) = &self.pending_placement {
            return if pending.request == *request {
                Ok(())
            } else if pending.request.operation == request.operation
                && pending.request.kind == request.kind
            {
                Err(LedgerError::PlacementConflict)
            } else {
                Err(LedgerError::Capacity)
            };
        }
        if self.pending_count() != 0 {
            return Err(LedgerError::Capacity);
        }
        let allocation = self
            .budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                PLACEMENT_WORKSPACE,
            )?
            .commit();
        let configuration = self.membership()?.configuration;
        self.validate_placement_transition(request, &configuration)?;
        let record = PlacementRecord {
            schema: 1,
            ledger: self.ledger,
            group: LogGroupId(self.group_id()),
            genesis: self.placement_genesis()?,
            sequence: self.sequence(),
            configuration,
            request: request.clone(),
        };
        let bytes = encode_placement(&record)?;
        self.consensus.propose_in(bytes, BudgetLane::Completion)?;
        self.pending_placement = Some(PendingPlacementRecord {
            request: request.clone(),
            _allocation: allocation,
        });
        Ok(())
    }
    fn validate_placement_transition(
        &self,
        request: &SessionPlacementRequest,
        configuration: &MembershipConfiguration,
    ) -> Result<(), LedgerError> {
        request.validate()?;
        configuration.validate()?;
        if request.expected_index
            != self
                .placement_state
                .latest()
                .map_or(0, |record| record.fence.index.0)
            || request.expected_configuration_index != self.membership_state.configuration_index
            || !configuration.voters_outgoing.is_empty()
            || !configuration.learners_next.is_empty()
            || configuration.auto_leave
            || !request
                .placement
                .placement
                .voters
                .keys()
                .all(|voter| configuration.voters.contains(voter))
        {
            // Every voter the placement names votes in the configuration; a
            // current voter the placement drops keeps its vote until the
            // activation retires it (24 §4, §19).
            return Err(LedgerError::PlacementConflict);
        }
        let active = self.placement_state.active.as_ref();
        match request.kind {
            SessionFenceKind::Created => {
                if active.is_some()
                    || self.placement_state.cutover.is_some()
                    || request.from_route != RouteEpoch(0)
                    || request.to_route != RouteEpoch(1)
                    || request.membership_epoch != 1
                    || request.placement_epoch != 1
                {
                    return Err(LedgerError::PlacementConflict);
                }
            }
            SessionFenceKind::Cutover => {
                let active = active.ok_or(LedgerError::PlacementConflict)?;
                let membership = active
                    .fence
                    .membership_epoch
                    .checked_add(u64::from(
                        active.record.request.placement.placement.voters
                            != request.placement.placement.voters,
                    ))
                    .ok_or(LedgerError::Capacity)?;
                // A cutover may follow several committed configuration changes;
                // its epoch is the group's actual epoch, never below the change
                // this placement itself implies.
                if self.placement_state.paused()
                    || request.operation == active.fence.operation
                    || request.from_route != active.fence.to_route
                    || request.membership_epoch < membership
                    || active.fence.placement_epoch.checked_add(1) != Some(request.placement_epoch)
                {
                    return Err(LedgerError::PlacementConflict);
                }
            }
            SessionFenceKind::Activated => {
                let cutover = self
                    .placement_state
                    .cutover
                    .as_ref()
                    .filter(|_| self.placement_state.paused())
                    .ok_or(LedgerError::PlacementConflict)?;
                let expected = &cutover.record.request;
                if request.operation != expected.operation
                    || request.from_route != expected.from_route
                    || request.to_route != expected.to_route
                    || request.membership_epoch != expected.membership_epoch
                    || request.placement_epoch != expected.placement_epoch
                    || request.placement != expected.placement
                    || configuration != &cutover.record.configuration
                {
                    return Err(LedgerError::PlacementConflict);
                }
            }
        }
        Ok(())
    }
    fn apply_placement_entry(
        &mut self,
        data: &[u8],
        index: u64,
        term: u64,
        configuration: &MembershipConfiguration,
    ) -> Result<(), LedgerError> {
        if data.len() > MAX_PLACEMENT_BYTES || index == 0 || term == 0 {
            return Err(LedgerError::Corrupt);
        }
        let mut allocation = self
            .budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                PLACEMENT_WORKSPACE,
            )?
            .commit();
        let (record, tail): (PlacementRecord, _) = durable_session_v1::take(
            data.strip_prefix(PLACEMENT_MAGIC)
                .ok_or(LedgerError::Corrupt)?,
        )?;
        if !tail.is_empty()
            || record.schema != 1
            || record.ledger != self.ledger
            || record.group != LogGroupId(self.group_id())
            || record.genesis != self.placement_genesis()?
            || record.sequence != self.sequence()
            || &record.configuration != configuration
        {
            return Err(LedgerError::Corrupt);
        }
        if let Some(existing) = self.placement_state.receipt(&record.request)? {
            if existing.record != record {
                return Err(LedgerError::Corrupt);
            }
            self.pending_placement = None;
            return Ok(());
        }
        self.validate_placement_transition(&record.request, configuration)
            .map_err(|_| LedgerError::Corrupt)?;
        if self.placement_state.latest().is_some_and(|old| {
            old.fence.index.0 >= index
                || old.fence.term.0 > term
                || old.fence.sequence > record.sequence
        }) {
            return Err(LedgerError::Corrupt);
        }
        let fence = record.fence(index, term, data)?;
        let stored = StoredPlacement { record, fence };
        let mut next = PlacementState {
            active: self.placement_state.active.clone(),
            cutover: self.placement_state.cutover.clone(),
        };
        match stored.fence.kind {
            SessionFenceKind::Cutover => next.cutover = Some(stored),
            _ => next.active = Some(stored),
        }
        allocation.shrink_to(placement_charge(&next)?)?;
        self.placement_state = next;
        self.placement_charge = Some(allocation);
        self.pending_placement = None;
        Ok(())
    }
    fn validate_placement_snapshot(
        &self,
        state: &PlacementState,
        index: u64,
        term: u64,
        sequence: SessionSeq,
    ) -> Result<(), LedgerError> {
        for stored in [&state.active, &state.cutover].into_iter().flatten() {
            let record = &stored.record;
            record.request.validate()?;
            let encoded = encode_placement(record)?;
            if record.schema != 1
                || record.ledger != self.ledger
                || record.group != LogGroupId(self.group_id())
                || record.genesis != self.placement_genesis()?
                || stored.fence.index.0 == 0
                || stored.fence.index.0 > index
                || stored.fence.term.0 == 0
                || stored.fence.term.0 > term
                || record.sequence > sequence
                || stored.fence
                    != record.fence(stored.fence.index.0, stored.fence.term.0, &encoded)?
                || record.request.expected_index >= stored.fence.index.0
                || record.request.expected_configuration_index >= stored.fence.index.0
            {
                return Err(LedgerError::Corrupt);
            }
            record.configuration.validate()?;
            if !record.configuration.voters.iter().copied().eq(record
                .request
                .placement
                .placement
                .voters
                .keys()
                .copied())
                || !record.configuration.voters_outgoing.is_empty()
                || !record.configuration.learners_next.is_empty()
                || record.configuration.auto_leave
            {
                return Err(LedgerError::Corrupt);
            }
        }
        if state.paused()
            && state
                .latest()
                .is_some_and(|entry| entry.fence.sequence != sequence)
        {
            return Err(LedgerError::Corrupt);
        }
        let Some(active) = &state.active else {
            return if state.cutover.is_none() {
                Ok(())
            } else {
                Err(LedgerError::Corrupt)
            };
        };
        if active.fence.kind == SessionFenceKind::Cutover {
            return Err(LedgerError::Corrupt);
        }
        match &state.cutover {
            None if active.fence.kind != SessionFenceKind::Created
                || active.fence.from_route != RouteEpoch(0)
                || active.fence.to_route != RouteEpoch(1)
                || active.fence.membership_epoch != 1
                || active.fence.placement_epoch != 1
                || active.record.request.expected_index != 0 =>
            {
                Err(LedgerError::Corrupt)
            }
            Some(cutover) => {
                if cutover.fence.kind != SessionFenceKind::Cutover
                    || cutover.fence.index == active.fence.index
                {
                    return Err(LedgerError::Corrupt);
                }
                if cutover.fence.index > active.fence.index {
                    let expected_membership = active
                        .fence
                        .membership_epoch
                        .checked_add(u64::from(
                            active.record.request.placement.placement.voters
                                != cutover.record.request.placement.placement.voters,
                        ))
                        .ok_or(LedgerError::Corrupt)?;
                    if cutover.fence.operation == active.fence.operation
                        || cutover.record.request.expected_index != active.fence.index.0
                        || cutover.fence.from_route != active.fence.to_route
                        || active.fence.placement_epoch.checked_add(1)
                            != Some(cutover.fence.placement_epoch)
                        || cutover.fence.membership_epoch < expected_membership
                        || cutover.fence.sequence < active.fence.sequence
                        || cutover.fence.term < active.fence.term
                    {
                        return Err(LedgerError::Corrupt);
                    }
                } else if active.fence.kind != SessionFenceKind::Activated
                    || active.record.request.expected_index != cutover.fence.index.0
                    || active.fence.operation != cutover.fence.operation
                    || active.fence.from_route != cutover.fence.from_route
                    || active.fence.to_route != cutover.fence.to_route
                    || active.fence.membership_epoch != cutover.fence.membership_epoch
                    || active.fence.placement_epoch != cutover.fence.placement_epoch
                    || active.record.request.placement != cutover.record.request.placement
                    || active.record.configuration != cutover.record.configuration
                    || active.fence.sequence < cutover.fence.sequence
                    || active.fence.term < cutover.fence.term
                {
                    return Err(LedgerError::Corrupt);
                }
                Ok(())
            }
            None => Ok(()),
        }
    }
}
impl PlacementRecord {
    fn fence(&self, index: u64, term: u64, encoded: &[u8]) -> Result<SessionFence, LedgerError> {
        let request = &self.request;
        Ok(SessionFence {
            kind: request.kind,
            ledger: self.ledger,
            log_group: self.group,
            operation: request.operation,
            sequence: self.sequence,
            index: RaftIndex(index),
            term: RaftTerm(term),
            from_route: request.from_route,
            to_route: request.to_route,
            membership_epoch: request.membership_epoch,
            placement_epoch: request.placement_epoch,
            placement_digest: focal_directory::placement_digest(&request.placement)
                .map_err(|_| LedgerError::PlacementConflict)?,
            record_hash: ContentHash(*blake3::hash(encoded).as_bytes()),
        })
    }
}
fn placement_charge(value: &impl Serialize) -> Result<usize, LedgerError> {
    postcard::experimental::serialized_size(value)?
        .checked_mul(8)
        .and_then(|n| n.checked_add(8192))
        .ok_or(LedgerError::Capacity)
}
fn encode_placement(record: &PlacementRecord) -> Result<Vec<u8>, LedgerError> {
    durable_session_v1::encode(PLACEMENT_MAGIC, record, MAX_PLACEMENT_BYTES)
}
