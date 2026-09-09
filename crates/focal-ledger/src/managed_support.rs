/// This descriptor enumerates actual decoders and snapshot retention contracts.
/// This floor is immutable once installed. A future format change needs an
/// explicit versioned floor transition, retained historical decoders, and a new
/// all-voter support barrier; replacing this descriptor alone cannot upgrade it.
const MANAGED_FORMAT_DESCRIPTOR: &[u8] = b"focal-managed/v1;model-managed-schema=1;domain=FOCALMD1:PreparedManagedMutation;cursor=FOCALMU1;control=FOCALMS1;snapshot=FOCALSS5;shared-family-window;last-used-generation;receipt-ack-full-v1";
fn managed_format_hash() -> [u8; 32] {
    *blake3::hash(MANAGED_FORMAT_DESCRIPTOR).as_bytes()
}
#[derive(Default)]
struct ManagedSupportCache {
    configuration_index: Option<u64>,
    demanded: bool,
    nodes: Vec<u64>,
    /// Peers whose durable floor already names the native successor decoder.
    native_nodes: Vec<u64>,
    _charge: Option<Allocation>,
    _native_charge: Option<Allocation>,
}
impl Session {
    /// The compiled immutable managed decoder descriptor; inspecting it never
    /// requests the irreversible durable floor or activates managed admission.
    pub fn managed_decoder_hash() -> [u8; 32] {
        managed_format_hash()
    }
    /// The actual fsynced floor of this physical group, without initiating IO.
    pub fn required_decoder(&self) -> Option<[u8; 32]> {
        self.consensus.required_decoder()
    }
    fn fence_managed_message(&mut self, message: &Message) -> Result<(), LedgerError> {
        if self.consensus.decoder_floor_ready(managed_format_hash()) {
            return Ok(());
        }
        if message.to != self.status().node_id || message.from == 0 {
            return Err(
                ConsensusError::Configuration("wrong destination or missing sender").into(),
            );
        }
        let managed = message.entries.iter().any(|entry| {
            entry.data.starts_with(MANAGED_DOMAIN_MAGIC)
                || entry.data.starts_with(MANAGED_CURSOR_MAGIC)
                || entry.data.starts_with(REQUEST_STREAM_MAGIC)
        }) || message.get_snapshot().data.starts_with(SNAPSHOT_V5_MAGIC);
        if managed {
            // Existing learners need not have participated in the initial voter
            // support barrier. Fence their first managed packet before Raft can
            // persist it. The caller reports dropped snapshots as failed; normal
            // Raft retransmission resumes after the local floor reaches disk.
            self.begin_managed_support()?;
            if !self.consensus.decoder_floor_ready(managed_format_hash()) {
                return Err(ConsensusError::PersistencePending.into());
            }
        }
        Ok(())
    }
    /// Requesting this capability opts the local physical group into an
    /// irreversible durable decoder floor. Poll before advertising the result.
    pub fn begin_managed_support(&mut self) -> Result<(), LedgerError> {
        self.check()?;
        self.managed_support.demanded = true;
        self.consensus.begin_decoder_floor(managed_format_hash())?;
        Ok(())
    }
    pub fn managed_support_demanded(&self) -> bool {
        self.managed_support.demanded
            || self.consensus.required_decoder() == Some(managed_format_hash())
    }

    /// Committed activation proves that the original voters durably promised
    /// this decoder. Every later addition/promotion is fenced by the same
    /// promise. Volatile observations need not be rebuilt after a leader restart.
    /// This is not readiness: callers still wait for the local durable floor,
    /// current-term authority, and any pending membership change.
    pub fn managed_protocol_active(&self) -> bool {
        self.request_streams.activated
    }

    pub fn needs_managed_support(&self, node: u64) -> bool {
        node != self.status().node_id
            && (self.managed_support.configuration_index
                != Some(self.membership_state.configuration_index)
                || !self.managed_support.nodes.contains(&node)
                || (self.hosting.is_some() && !self.managed_support.native_nodes.contains(&node)))
    }
    /// The descriptor this replica advertises: the native successor once its
    /// own transition is durable, otherwise the managed baseline. A hosted
    /// replica promises the successor as soon as it can, so an activation may
    /// later find every voter's promise already recorded.
    pub fn native_support(&self) -> Result<ManagedFormatSupport, LedgerError> {
        let mut fact = self.managed_support()?;
        if self.consensus.decoder_floor_ready(native_format_hash()) {
            fact.format_hash = ContentHash(native_format_hash());
        }
        Ok(fact)
    }
    /// Every current voter, in both sets of a joint configuration, has a
    /// recorded durable promise of the native successor decoder.
    fn require_native_support(&self) -> Result<(), LedgerError> {
        if !self.consensus.decoder_floor_ready(native_format_hash()) {
            return Err(ConsensusError::PersistencePending.into());
        }
        if self.pending_membership.is_some() {
            return Err(ManagedError::Unsupported.into());
        }
        let current = self.membership()?;
        if current.configuration.voters.is_empty() {
            return Err(ManagedError::Unsupported.into());
        }
        let local = self.status().node_id;
        for node in current
            .configuration
            .voters
            .iter()
            .chain(&current.configuration.voters_outgoing)
        {
            if *node != local
                && (self.managed_support.configuration_index != Some(current.configuration_index)
                    || !self.managed_support.native_nodes.contains(node))
            {
                return Err(ManagedError::Unsupported.into());
            }
        }
        Ok(())
    }
    /// After activation, a new learner or promoted voter must already promise
    /// the native successor; the managed baseline alone is not enough.
    fn native_membership_guard(&self, change: MembershipChange) -> Result<(), LedgerError> {
        if !self.activation.is_native() {
            return Ok(());
        }
        if let MembershipChange::AddLearner { node } | MembershipChange::Promote { node } = change
            && (self.managed_support.configuration_index
                != Some(self.membership_state.configuration_index)
                || !self.managed_support.native_nodes.contains(&node))
        {
            return Err(ManagedError::Unsupported.into());
        }
        Ok(())
    }
    pub fn managed_support(&self) -> Result<ManagedFormatSupport, LedgerError> {
        if !self.consensus.decoder_floor_ready(managed_format_hash()) {
            return Err(ConsensusError::PersistencePending.into());
        }
        let membership = self.membership()?;
        let configuration = membership.configuration;
        Ok(ManagedFormatSupport {
            cluster: self.cluster_id(),
            ledger: self.ledger,
            group: self.group_id(),
            node: self.status().node_id,
            configuration_index: membership.configuration_index,
            voters: configuration.voters,
            voters_outgoing: configuration.voters_outgoing,
            learners: configuration.learners,
            learners_next: configuration.learners_next,
            auto_leave: configuration.auto_leave,
            format_hash: ContentHash(managed_format_hash()),
        })
    }
    /// Trusted transport composition only. The peer ID must come from the
    /// authenticated connection; Runtime/Actor request payloads cannot call this.
    /// Existing members bind the exact current configuration. A prospective
    /// learner may report its actual immutable bootstrap configuration at index zero;
    /// its support is cached against this owner's current configuration and is
    /// invalidated by the addition. It cannot count for a current voter.
    pub fn record_managed_support(
        &mut self,
        authenticated_peer_node: u64,
        fact: ManagedFormatSupport,
    ) -> Result<(), LedgerError> {
        self.check()?;
        let expected = self.managed_support()?;
        let is_member = expected
            .voters
            .iter()
            .chain(&expected.voters_outgoing)
            .chain(&expected.learners)
            .chain(&expected.learners_next)
            .any(|node| *node == authenticated_peer_node);
        let exact_configuration = fact.configuration_index == expected.configuration_index
            && fact.voters == expected.voters
            && fact.voters_outgoing == expected.voters_outgoing
            && fact.learners == expected.learners
            && fact.learners_next == expected.learners_next
            && fact.auto_leave == expected.auto_leave;
        let (bootstrap_voters, bootstrap_learners) = self.consensus.bootstrap_membership();
        let same_bootstrap = |actual: &[u64], original: &[u64]| {
            actual.len() == original.len()
                && actual.windows(2).all(|pair| matches!(pair,[a,b] if a<b))
                && original
                    .iter()
                    .all(|node| actual.binary_search(node).is_ok())
        };
        let prospective = !is_member
            && fact.configuration_index == 0
            && same_bootstrap(&fact.voters, bootstrap_voters)
            && same_bootstrap(&fact.learners, bootstrap_learners)
            && fact.voters_outgoing.is_empty()
            && fact.learners_next.is_empty()
            && !fact.auto_leave;
        let native = fact.format_hash == ContentHash(native_format_hash());
        if authenticated_peer_node == 0
            || authenticated_peer_node != fact.node
            || fact.cluster != expected.cluster
            || fact.ledger != expected.ledger
            || fact.group != expected.group
            || !(exact_configuration || prospective)
            || !(fact.format_hash == expected.format_hash || native)
        {
            return Err(ManagedError::Unsupported.into());
        }
        if self.managed_support.configuration_index != Some(expected.configuration_index) {
            self.managed_support = ManagedSupportCache::default();
            self.managed_support.configuration_index = Some(expected.configuration_index);
        }
        if native && !self.managed_support.native_nodes.contains(&authenticated_peer_node) {
            // A successor promise implies the managed baseline it was written over.
            if self.managed_support.native_nodes.len() >= 1025 {
                return Err(ManagedError::Capacity.into());
            }
            if self.managed_support.native_nodes.capacity() < 1025 {
                let charge = self
                    .budget
                    .reserve(BudgetKind::Control, BudgetLane::Completion, 1025 * 8 + 128)?
                    .commit();
                self.managed_support
                    .native_nodes
                    .try_reserve_exact(1025usize.saturating_sub(self.managed_support.native_nodes.len()))
                    .map_err(|_| ManagedError::Capacity)?;
                self.managed_support._native_charge = Some(charge);
            }
            self.managed_support.native_nodes.push(authenticated_peer_node);
        }
        if self
            .managed_support
            .nodes
            .contains(&authenticated_peer_node)
        {
            return Ok(());
        }
        if prospective
            && self.managed_support.nodes.iter().any(|node| {
                !expected.voters.contains(node)
                    && !expected.voters_outgoing.contains(node)
                    && !expected.learners.contains(node)
                    && !expected.learners_next.contains(node)
            })
        {
            return Err(ManagedError::Capacity.into());
        }
        // The consensus configuration has the same fixed 1024-member bound;
        // retain one additional prospective learner, not an unbounded probe map.
        if self.managed_support.nodes.len() >= 1025 {
            return Err(ManagedError::Capacity.into());
        }
        if self.managed_support.nodes.capacity() < 1025 {
            let charge = self
                .budget
                .reserve(BudgetKind::Control, BudgetLane::Completion, 1025 * 8 + 128)?
                .commit();
            self.managed_support
                .nodes
                .try_reserve_exact(1025usize.saturating_sub(self.managed_support.nodes.len()))
                .map_err(|_| ManagedError::Capacity)?;
            self.managed_support._charge = Some(charge);
        }
        self.managed_support.nodes.push(authenticated_peer_node);
        Ok(())
    }
    fn require_managed_support(&mut self) -> Result<(), LedgerError> {
        self.begin_managed_support()?;
        if !self.consensus.decoder_floor_ready(managed_format_hash()) {
            return Err(ConsensusError::PersistencePending.into());
        }
        if self.pending_membership.is_some() {
            return Err(ManagedError::Unsupported.into());
        }
        let current = self.membership()?;
        if current.configuration.voters.is_empty() {
            return Err(ManagedError::Unsupported.into());
        }
        if self.managed_protocol_active() {
            return Ok(());
        }
        let local = self.status().node_id;
        for node in current
            .configuration
            .voters
            .iter()
            .chain(&current.configuration.voters_outgoing)
        {
            if *node != local
                && (self.managed_support.configuration_index != Some(current.configuration_index)
                    || !self.managed_support.nodes.contains(node))
            {
                return Err(ManagedError::Unsupported.into());
            }
        }
        Ok(())
    }
    fn managed_membership_guard(&self, change: MembershipChange) -> Result<(), LedgerError> {
        if !self.request_streams.activated {
            return Ok(());
        }
        if let MembershipChange::AddLearner { node } | MembershipChange::Promote { node } = change
            && (self.managed_support.configuration_index
                != Some(self.membership_state.configuration_index)
                || !self.managed_support.nodes.contains(&node))
        {
            return Err(ManagedError::Unsupported.into());
        }
        Ok(())
    }
}
