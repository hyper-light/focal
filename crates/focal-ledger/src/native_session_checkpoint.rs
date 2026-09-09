//! Enclosing checkpoint publication and authoritative snapshot installation for
//! the native Session. A checkpoint encloses only the committed native Core at
//! the fully delivered Raft prefix; a pending speculative suffix stays in Raft.
//! Installation rebuilds a complete replacement root before replacing the domain,
//! so a refused restoration leaves the previous authoritative state intact.
use super::engine::{Domain, NativeEngine};
use super::*;
use crate::native_checkpoint::{self as enclosing, Activation, AncillaryProfile, Metadata};
use focal_consensus::AppliedSnapshot;

/// The producer incarnation of a replica that installed an authoritative
/// snapshot: derived from the attested genesis and the exact install event, so
/// it is reproducible from the log, distinct from the checkpoint's own producer,
/// and never serialized as durable identity. Only later records name it.
fn incarnation(
    genesis: &ContentHash,
    node: u64,
    index: u64,
    term: u64,
) -> Result<RangeId, NativeSessionError> {
    let mut hasher = blake3::Hasher::new_derive_key("focal.native.session.incarnation.v1");
    hasher.update(&genesis.0);
    hasher.update(&node.to_le_bytes());
    hasher.update(&index.to_le_bytes());
    hasher.update(&term.to_le_bytes());
    let digest = hasher.finalize();
    let bytes: [u8; 16] = digest.as_bytes()[..16]
        .try_into()
        .map_err(|_| NativeSessionError::Corrupt)?;
    let value = u128::from_le_bytes(bytes);
    if value == 0 {
        return Err(NativeSessionError::Corrupt);
    }
    Ok(RangeId(value))
}

impl<S: NativeSchemaVerifier> NativeEngine<S> {
    /// The durable decoder floor this group actually promised. Constructing the
    /// checkpoint metadata from anything else would bind a capability the log
    /// never recorded.
    fn durable_floor(&self, consensus: &DurableNode) -> Result<ContentHash, NativeSessionError> {
        let floor = consensus
            .required_decoder()
            .ok_or(NativeSessionError::Legacy)?;
        if floor != decoder() {
            return Err(NativeSessionError::Legacy);
        }
        Ok(ContentHash(floor))
    }
    fn activation(&self, consensus: &DurableNode) -> Result<Activation, NativeSessionError> {
        let decoder = enclosing::format_hash();
        let derived = enclosing::genesis(
            consensus.cluster_id(),
            consensus.group_id(),
            self.ledger,
            self.profile,
            decoder,
        );
        if self.genesis.is_some_and(|applied| applied != derived) {
            return Err(NativeSessionError::Corrupt);
        }
        Ok(Activation {
            decoder,
            durable_floor: self.durable_floor(consensus)?,
            genesis: derived,
        })
    }

    /// Encode the committed Core at the fully delivered prefix under a retained
    /// output permit and hand bytes and permit together to consensus. Completion
    /// requires the actual durable fence observed by `poll`/`try_poll`.
    pub(crate) fn begin_checkpoint(
        &mut self,
        consensus: &mut DurableNode,
    ) -> Result<(), NativeSessionError> {
        self.check()?;
        if self.delivery.is_some() || consensus.persistence_pending() {
            return Err(NativeSessionError::Consensus(
                ConsensusError::PersistencePending,
            ));
        }
        let (bytes, allocation) = self.encode_checkpoint(consensus)?;
        consensus.begin_checkpoint_funded(self.applied_raft, bytes, allocation)?;
        Ok(())
    }
    /// The enclosing native checkpoint of the committed Core at the fully
    /// delivered prefix, with its output permit. A hosting Session nests these
    /// bytes inside its own envelope; the standalone session hands them to
    /// consensus directly.
    pub(crate) fn encode_checkpoint(
        &self,
        consensus: &DurableNode,
    ) -> Result<(Vec<u8>, Allocation), NativeSessionError> {
        self.check()?;
        let status = consensus.status();
        if self.applied_raft == 0
            || self.applied_raft != status.applied_index
            || self.genesis.is_none()
        {
            return Err(NativeSessionError::Consensus(
                ConsensusError::CheckpointIndex,
            ));
        }
        let applied_term = consensus.published_term(self.applied_raft)?;
        let configuration = consensus.membership_configuration();
        let metadata = Metadata {
            cluster: consensus.cluster_id(),
            group: consensus.group_id(),
            applied_raft: self.applied_raft,
            applied_term,
            configuration_index: self.configuration_index,
            recording_range: self.recording_range,
            recording_term: self.recording_term,
            records_floor: self.records_floor.0,
            activation_index: self.activation_index,
            activation: self.activation(consensus)?,
            ancillary: AncillaryProfile::NativeOnlyV1,
        };
        let core = match self.domain.as_ref() {
            Some(Domain::Passive(core)) => core,
            Some(Domain::Active(owner, _)) => owner.committed_core(),
            None => return Err(NativeSessionError::Failed),
        };
        let plan = enclosing::EncodingPlan::prepare(
            core,
            metadata,
            &configuration,
            self.limits.checkpoint,
        )?;
        Ok(plan.encode_in(&self.budget)?.into_parts())
    }

    /// Validate and install an authoritative snapshot. Every identity, floor,
    /// genesis and membership fact is checked before the Core is rebuilt; the
    /// previous domain is replaced only after complete construction succeeds.
    pub(super) fn restore_snapshot(
        &mut self,
        snapshot: &AppliedSnapshot,
        consensus: &DurableNode,
    ) -> Result<(), NativeSessionError> {
        self.restore_from(
            &snapshot.data,
            snapshot.index,
            snapshot.term,
            &snapshot.configuration,
            consensus,
        )
    }
    /// Install the enclosing native checkpoint bytes an authoritative snapshot
    /// carried, at its exact Raft coordinates and membership configuration.
    pub(crate) fn restore_from(
        &mut self,
        data: &[u8],
        index: u64,
        term: u64,
        configuration: &focal_consensus::MembershipConfiguration,
        consensus: &DurableNode,
    ) -> Result<(), NativeSessionError> {
        let checkpoint = enclosing::Checkpoint::inspect(data, self.limits.checkpoint)?;
        let header = checkpoint.header();
        let meta = header.metadata;
        if meta.ancillary != AncillaryProfile::NativeOnlyV1 {
            return Err(NativeSessionError::Legacy);
        }
        if meta.cluster != consensus.cluster_id()
            || meta.group != consensus.group_id()
            || header.ledger != self.ledger
            || header.profile != self.profile
            || meta.activation != self.activation(consensus)?
            || meta.applied_raft != index
            || meta.applied_term != term
            || meta.applied_raft < self.applied_raft
        {
            return Err(NativeSessionError::Corrupt);
        }
        checkpoint.configuration_matches(configuration)?;
        let range = incarnation(
            &meta.activation.genesis,
            consensus.status().node_id,
            index,
            term,
        )?;
        let restored = recovery::restore(
            checkpoint.core(),
            range,
            self.limits.recovery,
            self.budget.clone(),
            &self.reader,
            &self.schemas,
        )?;
        if restored.native_sequence() != header.prefix {
            return Err(NativeSessionError::Corrupt);
        }
        // An installed authoritative snapshot resolves every speculative candidate.
        self.resolve_suffix(super::apply::SuffixEvidence::InstalledSnapshot)?;
        self.readiness_requested = None;
        self.reconstruction_needed = true;
        self.domain = Some(Domain::Passive(restored));
        self.range = range;
        self.recording_range = meta.recording_range;
        self.recording_term = meta.recording_term;
        self.records_floor = SessionSeq(meta.records_floor);
        self.activation_index = meta.activation_index;
        self.configuration_index = meta.configuration_index;
        self.applied_raft = meta.applied_raft;
        // A checkpoint exists only after the committed genesis it attests.
        self.genesis = Some(meta.activation.genesis);
        Ok(())
    }
}
