// Native activation inside the unified Session: a committed activation record
// switches the domain engine from the frozen V1 reducer to the native engine
// while every ancillary protocol (cursors, deltas, membership, placement,
// managed streams, evidence snapshots) keeps its state and identities.
// Included into `session.rs`; nothing here owns consensus or content.

const ACTIVATION_MAGIC: &[u8; 8] = b"FOCALAC1";
const ACTIVATION_SCHEMA: u16 = 2;
const ACTIVATION_BYTES: usize =
    8 + 2 + 1 + 1 + 32 + 32 + 8 + 32 + 8 + 8 + 32 + 8 + 8 + 8 + 32 + 32;
const IMPORT_INTENT_DOMAIN: &str = "focal.session.import.intent.v1";
const ACTIVATION_HASH_DOMAIN: &str = "focal.session.activation.v1";

fn native_format_hash() -> [u8; 32] {
    crate::native_checkpoint::format_hash().0
}

/// `FOCALSS6`: every SS5 section, the applied activation record and the
/// enclosing native checkpoint. Legacy sections keep their frozen codecs.
struct SnapshotEnvelopeV6 {
    state: SnapshotEnvelopeV4,
    requests: RequestStreamsCheckpoint,
    activation: Vec<u8>,
    native: Vec<u8>,
}
/// `FOCALSS7`: the SS6 sections plus the request-stream registry's generation
/// watermark, which a registry that evicts closed pairs at capacity needs to
/// keep every retired generation fenced (15 §"Session owner and controls").
struct SnapshotEnvelopeV7 {
    state: SnapshotEnvelopeV4,
    requests: RequestStreamsCheckpoint,
    activation: Vec<u8>,
    native: Vec<u8>,
    slot_generation: u64,
}

/// Physical resources and limits a node supplies so this Session can host the
/// native engine once activation commits. Without it, a native activation is
/// refused at ingress and this replica never persists native history.
pub struct NativeHosting {
    pub limits: NativeSessionLimits,
    pub reader: ContentReader,
    pub range: RangeId,
}

/// How this ledger's native history began.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivationKind {
    /// An empty legacy prefix; native history starts at the activation index.
    Genesis,
    /// Legacy objects were translated into native rows with provenance markers.
    Imported,
}
/// The committed domain engine of this ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LedgerActivation {
    V1,
    Native {
        kind: ActivationKind,
        profile: NativeContentProfile,
        /// Raft index of the committed activation record.
        index: u64,
    },
}
impl LedgerActivation {
    pub fn is_native(self) -> bool {
        matches!(self, Self::Native { .. })
    }
}

/// Custody for a native admission: the exclusive writer verifies and seals
/// evidence itself, or the host has already verified it and hands the token.
pub enum NativeCustody<'a> {
    Store(&'a mut ContentStore),
    Evidence(Option<&'a VerifiedNativeArtifact>),
}

/// `FOCALAC1`: the replicated activation fact. It names the exact predecessor
/// and successor decoders, the configuration it was proposed under, and the
/// legacy prefix it seals; a checksum trailer covers every field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ActivationRecord {
    pub(crate) kind: ActivationKind,
    pub(crate) profile: NativeContentProfile,
    pub(crate) predecessor: [u8; 32],
    pub(crate) successor: [u8; 32],
    pub(crate) configuration_index: u64,
    pub(crate) configuration_hash: [u8; 32],
    pub(crate) v1_sequence: u64,
    pub(crate) v1_applied_raft: u64,
    pub(crate) v1_checkpoint: [u8; 32],
    /// Import parameters every replica must reuse: the trusted logical time of
    /// the import outcome and the canonical chunking of inline payloads.
    pub(crate) logical_time: u64,
    pub(crate) chunk_bytes: u64,
    pub(crate) max_manifest_bytes: u64,
    pub(crate) native_root: [u8; 32],
}
fn put(target: &mut [u8], at: &mut usize, bytes: &[u8]) -> Result<(), LedgerError> {
    let end = at.checked_add(bytes.len()).ok_or(LedgerError::Corrupt)?;
    target
        .get_mut(*at..end)
        .ok_or(LedgerError::Corrupt)?
        .copy_from_slice(bytes);
    *at = end;
    Ok(())
}
fn take_bytes<'a>(source: &'a [u8], at: &mut usize, len: usize) -> Result<&'a [u8], LedgerError> {
    let end = at.checked_add(len).ok_or(LedgerError::Corrupt)?;
    let bytes = source.get(*at..end).ok_or(LedgerError::Corrupt)?;
    *at = end;
    Ok(bytes)
}
fn take_u64(source: &[u8], at: &mut usize) -> Result<u64, LedgerError> {
    let bytes = take_bytes(source, at, 8)?;
    Ok(u64::from_le_bytes(
        <[u8; 8]>::try_from(bytes).map_err(|_| LedgerError::Corrupt)?,
    ))
}
fn take_hash(source: &[u8], at: &mut usize) -> Result<[u8; 32], LedgerError> {
    let bytes = take_bytes(source, at, 32)?;
    <[u8; 32]>::try_from(bytes).map_err(|_| LedgerError::Corrupt)
}
/// A canonical identity of the joint configuration an activation was proposed
/// under; every replica recomputes it from its own applied configuration.
pub(crate) fn configuration_hash(configuration: &focal_consensus::MembershipConfiguration) -> [u8; 32] {
    let mut hash = blake3::Hasher::new_derive_key("focal.session.configuration.v1");
    for (tag, nodes) in [
        (1u8, &configuration.voters),
        (2u8, &configuration.voters_outgoing),
        (3u8, &configuration.learners),
        (4u8, &configuration.learners_next),
    ] {
        hash.update(&[tag]);
        hash.update(&(nodes.len() as u64).to_le_bytes());
        for node in nodes {
            hash.update(&node.to_le_bytes());
        }
    }
    hash.update(&[u8::from(configuration.auto_leave)]);
    *hash.finalize().as_bytes()
}
impl ActivationRecord {
    pub(crate) fn encode(&self) -> Result<[u8; ACTIVATION_BYTES], LedgerError> {
        let mut bytes = [0u8; ACTIVATION_BYTES];
        let mut at = 0usize;
        put(&mut bytes, &mut at, ACTIVATION_MAGIC)?;
        put(&mut bytes, &mut at, &ACTIVATION_SCHEMA.to_le_bytes())?;
        put(
            &mut bytes,
            &mut at,
            &[match self.kind {
                ActivationKind::Genesis => 1,
                ActivationKind::Imported => 2,
            }],
        )?;
        put(
            &mut bytes,
            &mut at,
            &[match self.profile {
                NativeContentProfile::ProjectionOnly => 0,
                NativeContentProfile::AuthoredV1 => 1,
            }],
        )?;
        put(&mut bytes, &mut at, &self.predecessor)?;
        put(&mut bytes, &mut at, &self.successor)?;
        put(&mut bytes, &mut at, &self.configuration_index.to_le_bytes())?;
        put(&mut bytes, &mut at, &self.configuration_hash)?;
        put(&mut bytes, &mut at, &self.v1_sequence.to_le_bytes())?;
        put(&mut bytes, &mut at, &self.v1_applied_raft.to_le_bytes())?;
        put(&mut bytes, &mut at, &self.v1_checkpoint)?;
        put(&mut bytes, &mut at, &self.logical_time.to_le_bytes())?;
        put(&mut bytes, &mut at, &self.chunk_bytes.to_le_bytes())?;
        put(&mut bytes, &mut at, &self.max_manifest_bytes.to_le_bytes())?;
        put(&mut bytes, &mut at, &self.native_root)?;
        let digest = Self::digest(bytes.get(..at).ok_or(LedgerError::Corrupt)?);
        put(&mut bytes, &mut at, &digest)?;
        if at != ACTIVATION_BYTES {
            return Err(LedgerError::Corrupt);
        }
        Ok(bytes)
    }
    fn digest(body: &[u8]) -> [u8; 32] {
        let mut hash = blake3::Hasher::new_derive_key(ACTIVATION_HASH_DOMAIN);
        hash.update(body);
        *hash.finalize().as_bytes()
    }
    /// The intent identity of an import outcome: every field of the record
    /// except the root it produces, so the root can depend on it.
    pub(crate) fn import_intent(&self) -> Result<ContentHash, LedgerError> {
        let bytes = self.encode()?;
        let body = bytes
            .get(..ACTIVATION_BYTES.saturating_sub(64))
            .ok_or(LedgerError::Corrupt)?;
        let mut hash = blake3::Hasher::new_derive_key(IMPORT_INTENT_DOMAIN);
        hash.update(body);
        Ok(ContentHash(*hash.finalize().as_bytes()))
    }
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, LedgerError> {
        if bytes.len() != ACTIVATION_BYTES {
            return Err(LedgerError::Corrupt);
        }
        let body = bytes
            .get(..ACTIVATION_BYTES.saturating_sub(32))
            .ok_or(LedgerError::Corrupt)?;
        let trailer = bytes
            .get(ACTIVATION_BYTES.saturating_sub(32)..)
            .ok_or(LedgerError::Corrupt)?;
        if Self::digest(body) != trailer {
            return Err(LedgerError::Corrupt);
        }
        let mut at = 0usize;
        if take_bytes(body, &mut at, 8)? != ACTIVATION_MAGIC {
            return Err(LedgerError::Corrupt);
        }
        let schema = take_bytes(body, &mut at, 2)?;
        if schema != ACTIVATION_SCHEMA.to_le_bytes() {
            return Err(LedgerError::Corrupt);
        }
        let kind = match take_bytes(body, &mut at, 1)? {
            [1] => ActivationKind::Genesis,
            [2] => ActivationKind::Imported,
            _ => return Err(LedgerError::Corrupt),
        };
        let profile = match take_bytes(body, &mut at, 1)? {
            [0] => NativeContentProfile::ProjectionOnly,
            [1] => NativeContentProfile::AuthoredV1,
            _ => return Err(LedgerError::Corrupt),
        };
        let record = Self {
            kind,
            profile,
            predecessor: take_hash(body, &mut at)?,
            successor: take_hash(body, &mut at)?,
            configuration_index: take_u64(body, &mut at)?,
            configuration_hash: take_hash(body, &mut at)?,
            v1_sequence: take_u64(body, &mut at)?,
            v1_applied_raft: take_u64(body, &mut at)?,
            v1_checkpoint: take_hash(body, &mut at)?,
            logical_time: take_u64(body, &mut at)?,
            chunk_bytes: take_u64(body, &mut at)?,
            max_manifest_bytes: take_u64(body, &mut at)?,
            native_root: take_hash(body, &mut at)?,
        };
        if at != body.len() {
            return Err(LedgerError::Corrupt);
        }
        Ok(record)
    }
}

/// Inline legacy payloads and the content domain they must be sealed under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyImportPayloads {
    pub domain: focal_model::ContentDomainId,
    pub payloads: Vec<Vec<u8>>,
}

/// An import a replica still has to apply: what its host must seal first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingImport {
    pub chunk_bytes: u64,
    pub max_manifest_bytes: u64,
    pub payloads: usize,
}

/// The native engine rebuilt from a checkpoint's native section, its activation
/// state and the retained activation record bytes.
type RestoredNative = (
    Box<engine::NativeEngine<BuiltinNativeSchemas>>,
    LedgerActivation,
    Vec<u8>,
);

/// A native entry, activation record or native checkpoint that a replica must
/// not persist before its own successor decoder floor is durable.
fn carries_native_history(message: &Message) -> bool {
    message.entries.iter().any(|entry| {
        entry.data.starts_with(ACTIVATION_MAGIC) || engine::NativeEngine::<BuiltinNativeSchemas>::is_native_entry(&entry.data)
    }) || message.get_snapshot().data.starts_with(SNAPSHOT_V6_MAGIC)
        || message.get_snapshot().data.starts_with(SNAPSHOT_V7_MAGIC)
}

impl Session {
    pub fn activation(&self) -> LedgerActivation {
        self.activation
    }
    /// This replica can host the native engine once activation commits.
    pub fn native_hosted(&self) -> bool {
        self.hosting.is_some()
    }
    /// The compiled successor decoder identity this build can host.
    pub fn native_decoder_hash() -> ContentHash {
        crate::native_checkpoint::format_hash()
    }
    /// Current-term authority whose native owner is reconstructed past the
    /// committed readiness barrier and whose genesis is applied.
    pub fn native_authoritative(&self) -> bool {
        self.is_authoritative()
            && self.retained.is_none()
            && self
                .native
                .as_ref()
                .is_some_and(|engine| engine.is_authoritative(&self.consensus.status()))
    }
    fn native_engine(&self) -> Result<&engine::NativeEngine<BuiltinNativeSchemas>, LedgerError> {
        self.check()?;
        self.native
            .as_deref()
            .ok_or(LedgerError::NativeUnsupported)
    }
    /// Establish this replica's durable successor promise: the managed baseline
    /// floor, then the recorded transition to the native decoder. Every step is
    /// idempotent and may report persistence pending until the write is fsynced.
    pub fn begin_native_support(&mut self) -> Result<(), LedgerError> {
        self.check()?;
        if self.hosting.is_none() {
            return Err(LedgerError::NativeUnsupported);
        }
        if self.consensus.decoder_floor_ready(native_format_hash()) {
            return Ok(());
        }
        self.begin_managed_support()?;
        if !self.consensus.decoder_floor_ready(managed_format_hash()) {
            return Err(ConsensusError::PersistencePending.into());
        }
        self.consensus
            .confirm_decoder_pair(managed_format_hash(), native_format_hash())?;
        self.consensus.begin_decoder_transition()?;
        Ok(())
    }
    pub fn native_support_ready(&self) -> bool {
        self.consensus.decoder_floor_ready(native_format_hash())
    }
    /// Refuse native history before Raft persists it unless this replica's
    /// successor floor is durable. The transport drops the packet and Raft
    /// retransmits once the floor reaches disk; a replica without native
    /// hosting refuses permanently, which keeps a mixed-version group honest.
    fn fence_native_message(&mut self, message: &Message) -> Result<(), LedgerError> {
        if self.consensus.decoder_floor_ready(native_format_hash())
            || !carries_native_history(message)
        {
            return Ok(());
        }
        if self.hosting.is_none() {
            return Err(LedgerError::NativeUnsupported);
        }
        self.begin_native_support()?;
        if !self.consensus.decoder_floor_ready(native_format_hash()) {
            return Err(ConsensusError::PersistencePending.into());
        }
        Ok(())
    }
    /// Propose the replicated activation of native history over an empty legacy
    /// prefix. The local successor promise must already be durable; the record
    /// commits under the current configuration and applies on every replica.
    pub fn propose_native_activation(
        &mut self,
        profile: NativeContentProfile,
    ) -> Result<(), LedgerError> {
        self.check_activation_proposal()?;
        // Genesis activation seals an empty legacy prefix; populated history
        // needs the import path, which records what it translated.
        if self.core.sequence() != SessionSeq(0)
            || !self.core.snapshot().receipts.is_empty()
            || !self.core.snapshot().epochs.is_empty()
        {
            return Err(LedgerError::ActivationConflict);
        }
        let membership = self.activation_barrier()?;
        let record = ActivationRecord {
            kind: ActivationKind::Genesis,
            profile,
            predecessor: managed_format_hash(),
            successor: native_format_hash(),
            configuration_index: membership.configuration_index,
            configuration_hash: configuration_hash(&membership.configuration),
            v1_sequence: self.core.sequence().0,
            v1_applied_raft: self.applied_raft,
            v1_checkpoint: [0; 32],
            logical_time: 0,
            chunk_bytes: 0,
            max_manifest_bytes: 0,
            native_root: [0; 32],
        };
        self.submit_activation(record)
    }
    /// Propose the replicated import of a populated legacy prefix (23 §5). The
    /// exclusive writer seals every inline legacy payload with the canonical
    /// chunk size, the translation runs here so the record carries the root
    /// every replica must reproduce, and no legacy proposal is admitted until
    /// the record commits or this authority changes.
    pub fn propose_native_import(
        &mut self,
        profile: NativeContentProfile,
        store: Option<&mut ContentStore>,
        logical_time: u64,
        chunk_bytes: usize,
        max_manifest_bytes: usize,
    ) -> Result<(), LedgerError> {
        self.check_activation_proposal()?;
        if !self.legacy_populated() {
            return Err(LedgerError::ActivationConflict);
        }
        if profile != NativeContentProfile::ProjectionOnly || chunk_bytes == 0 {
            return Err(LedgerError::ActivationConflict);
        }
        let membership = self.activation_barrier()?;
        let mut record = ActivationRecord {
            kind: ActivationKind::Imported,
            profile,
            predecessor: managed_format_hash(),
            successor: native_format_hash(),
            configuration_index: membership.configuration_index,
            configuration_hash: configuration_hash(&membership.configuration),
            v1_sequence: self.core.sequence().0,
            v1_applied_raft: self.applied_raft,
            v1_checkpoint: self.legacy_checkpoint_hash()?.0,
            logical_time,
            chunk_bytes: u64::try_from(chunk_bytes).map_err(|_| LedgerError::Capacity)?,
            max_manifest_bytes: u64::try_from(max_manifest_bytes)
                .map_err(|_| LedgerError::Capacity)?,
            native_root: [0; 32],
        };
        if let Some(store) = store {
            self.seal_import_payloads_with(store, chunk_bytes)?;
        }
        // Without the writer, the host sealed the payloads already; a missing
        // one surfaces here as a typed custody refusal, never as a proposal.
        let imported = self.translate_legacy(&record)?;
        record.native_root = imported.root.0;
        drop(imported);
        self.submit_activation(record)
    }
    /// Whether the legacy prefix holds history that activation must import.
    pub fn legacy_populated(&self) -> bool {
        self.core.sequence() != SessionSeq(0)
    }
    /// Inline legacy payloads a host must seal before this ledger can import,
    /// copied under one bound so the exclusive content writer can install them
    /// without borrowing the Session.
    pub fn legacy_import_payloads(
        &self,
        max_total_bytes: usize,
    ) -> Result<LegacyImportPayloads, LedgerError> {
        let hosting = self.hosting.as_ref().ok_or(LedgerError::NativeUnsupported)?;
        let mut payloads = Vec::new();
        let mut total = 0usize;
        for payload in focal_core::native::inline_payloads(self.core.snapshot()) {
            total = total
                .checked_add(payload.bytes.len())
                .filter(|total| *total <= max_total_bytes)
                .ok_or(LedgerError::Capacity)?;
            let mut copy = Vec::new();
            copy.try_reserve_exact(payload.bytes.len())
                .map_err(|_| LedgerError::Capacity)?;
            copy.extend_from_slice(payload.bytes);
            payloads.try_reserve(1).map_err(|_| LedgerError::Capacity)?;
            payloads.push(copy);
        }
        Ok(LegacyImportPayloads {
            domain: hosting.limits.content_domain,
            payloads,
        })
    }
    fn check_activation_proposal(&mut self) -> Result<(), LedgerError> {
        self.check()?;
        if !self.is_authoritative() {
            return Err(LedgerError::NotReady {
                leader: self.status().leader_id,
            });
        }
        if self.hosting.is_none() {
            return Err(LedgerError::NativeUnsupported);
        }
        if self.activation.is_native() || self.pending_activation.is_some() {
            return Err(LedgerError::ActivationConflict);
        }
        if self.pending_count() != 0
            || self.retained.is_some()
            || self.persistence_pending()
            || self.placement_state.paused()
        {
            return Err(LedgerError::Capacity);
        }
        Ok(())
    }
    fn activation_barrier(&mut self) -> Result<MembershipView, LedgerError> {
        self.begin_native_support()?;
        if !self.consensus.decoder_floor_ready(native_format_hash()) {
            return Err(ConsensusError::PersistencePending.into());
        }
        self.require_native_support()?;
        self.membership()
    }
    fn submit_activation(&mut self, record: ActivationRecord) -> Result<(), LedgerError> {
        let bytes = record.encode()?;
        let permit = self
            .budget
            .reserve(
                BudgetKind::Pending,
                BudgetLane::Completion,
                ACTIVATION_BYTES.saturating_add(64),
            )?
            .commit();
        self.consensus
            .propose_in(bytes.to_vec(), BudgetLane::Completion)?;
        self.pending_activation = Some((record, permit));
        Ok(())
    }
    /// Apply a committed activation record at its Raft index.
    fn apply_activation(&mut self, entry: &focal_consensus::CommittedEntry) -> Result<(), LedgerError> {
        let record = ActivationRecord::decode(&entry.data)?;
        if let LedgerActivation::Native { index, .. } = self.activation {
            // A duplicate of the applied record from a concurrent proposer is inert.
            return if self
                .activation_record
                .as_ref()
                .is_some_and(|applied| applied.as_slice() == entry.data.as_slice())
                && index < entry.index
            {
                Ok(())
            } else {
                Err(LedgerError::Corrupt)
            };
        }
        if record.predecessor != managed_format_hash()
            || record.successor != native_format_hash()
            || record.configuration_index != self.membership_state.configuration_index
            || record.configuration_hash != configuration_hash(&self.consensus.membership_configuration())
            || record.v1_sequence != self.core.sequence().0
            || record.v1_applied_raft > entry.index
        {
            return Err(LedgerError::Corrupt);
        }
        match record.kind {
            ActivationKind::Genesis => {
                if self.core.sequence() != SessionSeq(0)
                    || !self.core.snapshot().receipts.is_empty()
                {
                    return Err(LedgerError::Corrupt);
                }
            }
            ActivationKind::Imported => {
                if self.core.sequence() == SessionSeq(0)
                    || record.profile != NativeContentProfile::ProjectionOnly
                    || record.v1_checkpoint != self.legacy_checkpoint_hash()?.0
                {
                    return Err(LedgerError::Corrupt);
                }
            }
        }
        if !self.consensus.decoder_floor_ready(native_format_hash()) {
            return Err(LedgerError::Corrupt);
        }
        let hosting = self.hosting.as_ref().ok_or(LedgerError::NativeUnsupported)?;
        let mut engine = engine::NativeEngine::new(
            self.ledger,
            hosting.range,
            record.profile,
            hosting.limits,
            &self.budget,
            hosting.reader.clone(),
            BuiltinNativeSchemas,
        )?;
        match record.kind {
            ActivationKind::Genesis => {
                engine.adopt_prefix(entry.index, self.membership_state.configuration_index);
            }
            ActivationKind::Imported => {
                // Every replica translates its own sealed legacy core; a root that
                // differs from the leader's is a divergent replica, never a
                // different history. Missing local custody retains the delivery.
                let imported = self.translate_legacy(&record)?;
                if imported.root.0 != record.native_root {
                    return Err(LedgerError::ActivationConflict);
                }
                engine.install_imported(
                    imported.core,
                    entry.index,
                    self.membership_state.configuration_index,
                )?;
            }
        }
        engine.set_activation_index(entry.index);
        let status = self.consensus.status();
        engine.observe(&status);
        // An authority already past this term's readiness barrier has applied
        // every earlier committed entry; activation is the next one in order,
        // so the owner is reconstructed here instead of at a later barrier.
        if status.role == StateRole::Leader && self.ready_term == Some(status.term) {
            engine.promote(status.term, &self.consensus)?;
        }
        let mut retained = Vec::new();
        retained
            .try_reserve_exact(entry.data.len())
            .map_err(|_| LedgerError::Capacity)?;
        retained.extend_from_slice(&entry.data);
        self.native = Some(Box::new(engine));
        self.activation = LedgerActivation::Native {
            kind: record.kind,
            profile: record.profile,
            index: entry.index,
        };
        self.activation_record = Some(retained);
        self.pending_activation = None;
        Ok(())
    }
    /// Identity of the sealed legacy core: the hash of its frozen checkpoint.
    fn legacy_checkpoint_hash(&self) -> Result<ContentHash, LedgerError> {
        let bytes = self.core.encode_checkpoint()?;
        Ok(ContentHash(*blake3::hash(&bytes).as_bytes()))
    }
    /// Run the deterministic translation of the sealed legacy core under an
    /// activation record's parameters. Retryable custody and memory refusals
    /// keep the delivery; every other refusal is typed and final.
    fn translate_legacy(
        &self,
        record: &ActivationRecord,
    ) -> Result<focal_core::native::Imported, LedgerError> {
        let hosting = self.hosting.as_ref().ok_or(LedgerError::NativeUnsupported)?;
        let chunk_bytes = usize::try_from(record.chunk_bytes).map_err(|_| LedgerError::Corrupt)?;
        let max_manifest_bytes =
            usize::try_from(record.max_manifest_bytes).map_err(|_| LedgerError::Corrupt)?;
        let request = focal_core::native::ImportRequest {
            ledger: self.ledger,
            logical_time: record.logical_time,
            intent: record.import_intent()?,
            content_domain: hosting.limits.content_domain,
            chunk_bytes,
            max_manifest_bytes,
            range: hosting.range,
            limits: &hosting.limits.recovery,
            encoding: hosting.limits.encoding,
            inspection: hosting.limits.inspection,
        };
        let budget = self
            .budget
            .child(hosting.limits.memory_bytes, hosting.limits.completion_reserve_bytes)?;
        focal_core::native::import(
            self.core.snapshot(),
            request,
            budget,
            &hosting.reader,
            &BuiltinNativeSchemas,
        )
        .map_err(|error| match error {
            focal_core::native::ImportError::Evidence(focal_evidence::NativeEvidenceError::Content(_))
            | focal_core::native::ImportError::Content(_) => {
                LedgerError::Native(NativeSessionError::CustodyPending)
            }
            focal_core::native::ImportError::Evidence(focal_evidence::NativeEvidenceError::Memory(memory))
            | focal_core::native::ImportError::Memory(memory) => {
                LedgerError::Native(NativeSessionError::Memory(memory))
            }
            other => LedgerError::Import(other),
        })
    }
    /// The import this replica is waiting to apply, if a retained delivery or a
    /// pending proposal names one: hosts seal its inline payloads with these
    /// parameters and poll again.
    pub fn pending_import(&self) -> Option<PendingImport> {
        let record = if let Some((record, _)) = &self.pending_activation {
            *record
        } else {
            let delivery = self.retained.as_ref()?;
            let entry = delivery.events.committed.get(delivery.entry)?;
            ActivationRecord::decode(&entry.data).ok()?
        };
        (record.kind == ActivationKind::Imported && !self.activation.is_native()).then(|| {
            PendingImport {
                chunk_bytes: record.chunk_bytes,
                max_manifest_bytes: record.max_manifest_bytes,
                payloads: focal_core::native::inline_payloads(self.core.snapshot()).count(),
            }
        })
    }
    /// Seal every inline legacy payload into the local content tree with the
    /// pending import's chunk size, so custody can be re-verified at apply.
    pub fn seal_import_payloads(&mut self, store: &mut ContentStore) -> Result<usize, LedgerError> {
        let pending = self.pending_import().ok_or(LedgerError::ActivationConflict)?;
        let chunk_bytes = usize::try_from(pending.chunk_bytes).map_err(|_| LedgerError::Corrupt)?;
        self.seal_import_payloads_with(store, chunk_bytes)
    }
    fn seal_import_payloads_with(
        &self,
        store: &mut ContentStore,
        chunk_bytes: usize,
    ) -> Result<usize, LedgerError> {
        let domain = self
            .hosting
            .as_ref()
            .ok_or(LedgerError::NativeUnsupported)?
            .limits
            .content_domain;
        let mut sealed = 0usize;
        for payload in focal_core::native::inline_payloads(self.core.snapshot()) {
            store
                .seal_import_inline(domain, payload.bytes, chunk_bytes)
                .map_err(|error| {
                    LedgerError::Native(NativeSessionError::Native(
                        focal_core::native::NativeError::Evidence(
                            focal_evidence::NativeEvidenceError::Content(error),
                        ),
                    ))
                })?;
            sealed = sealed.checked_add(1).ok_or(LedgerError::Capacity)?;
        }
        Ok(sealed)
    }
    /// Rebuild the native engine from the native section of an installed
    /// checkpoint. Everything is constructed before the Session adopts it.
    fn native_from_checkpoint(
        &self,
        activation: &[u8],
        native: &[u8],
        index: u64,
        term: u64,
        configuration: &focal_consensus::MembershipConfiguration,
    ) -> Result<RestoredNative, LedgerError> {
        let record = ActivationRecord::decode(activation)?;
        if record.predecessor != managed_format_hash()
            || record.successor != native_format_hash()
            || record.v1_applied_raft > index
        {
            return Err(LedgerError::Corrupt);
        }
        let hosting = self.hosting.as_ref().ok_or(LedgerError::NativeUnsupported)?;
        let mut engine = engine::NativeEngine::new(
            self.ledger,
            hosting.range,
            record.profile,
            hosting.limits,
            &self.budget,
            hosting.reader.clone(),
            BuiltinNativeSchemas,
        )?;
        engine.observe(&self.consensus.status());
        engine.restore_from(native, index, term, configuration, &self.consensus)?;
        // The checkpoint retains the exact activation position (22 §FCNSESS);
        // it must lie between the sealed legacy prefix and the applied index.
        let activation_index = engine.activation_index();
        if activation_index <= record.v1_applied_raft || activation_index > index {
            return Err(LedgerError::Corrupt);
        }
        let mut retained = Vec::new();
        retained
            .try_reserve_exact(activation.len())
            .map_err(|_| LedgerError::Capacity)?;
        retained.extend_from_slice(activation);
        Ok((
            Box::new(engine),
            LedgerActivation::Native {
                kind: record.kind,
                profile: record.profile,
                index: activation_index,
            },
            retained,
        ))
    }

    fn admit_native(
        &mut self,
        prepare: impl FnOnce(
            &mut NativeOwner,
            &BuiltinNativeSchemas,
            ContentDomainId,
        ) -> Result<NativeStaging, NativeOwnerError>,
    ) -> Result<NativeSubmission, LedgerError> {
        self.check()?;
        if !self.is_authoritative() || self.retained.is_some() {
            return Err(LedgerError::NotReady {
                leader: self.status().leader_id,
            });
        }
        let engine = self
            .native
            .as_deref_mut()
            .ok_or(LedgerError::NativeUnsupported)?;
        let submission = engine.admit(&mut self.consensus, prepare)?;
        if engine.failed() {
            self.failed = true;
        }
        Ok(submission)
    }
    /// Admit one typed native input on the authority. Evidence custody is
    /// either sealed by the exclusive writer here or was verified by the host.
    pub fn propose_native(
        &mut self,
        context: NativeContext,
        input: NativeInput,
        custody: NativeCustody<'_>,
    ) -> Result<NativeSubmission, LedgerError> {
        match custody {
            NativeCustody::Store(store) => self.admit_native(|owner, schemas, domain| {
                owner.prepare_with_custody(context, input, store, domain, schemas)
            }),
            NativeCustody::Evidence(evidence) => self.admit_native(|owner, schemas, _| {
                owner.prepare_evidenced_with_schemas(context, input, evidence, schemas)
            }),
        }
    }
    /// Admit one borrowed native input frame from an authenticated participant.
    pub fn propose_native_frame(
        &mut self,
        context: NativeContext,
        frame: &[u8],
        custody: NativeCustody<'_>,
    ) -> Result<NativeSubmission, LedgerError> {
        let limits = self.native_engine()?.decode_limits()?;
        match custody {
            NativeCustody::Store(store) => self.admit_native(|owner, schemas, domain| {
                owner.prepare_frame_with_custody(context, frame, limits, store, domain, schemas)
            }),
            NativeCustody::Evidence(evidence) => self.admit_native(|owner, schemas, _| {
                owner.prepare_frame_evidenced_with_schemas(context, frame, limits, evidence, schemas)
            }),
        }
    }
    /// Trusted timer delivery from the host's clock.
    pub fn deliver_native_timer(
        &mut self,
        input: NativeTimerInput,
        logical_time: u64,
    ) -> Result<NativeSubmission, LedgerError> {
        self.admit_native(|owner, _, _| match input {
            NativeTimerInput::Evaluation(input) => {
                owner.prepare_evaluation_deadline(input, logical_time)
            }
            NativeTimerInput::Claim(input) => owner.prepare_claim_deadline(input, logical_time),
            NativeTimerInput::Monitor(input) => owner.prepare_monitor_deadline(input, logical_time),
        })
    }
    pub fn native_outcome(
        &self,
        request: impl Into<NativeInvocation>,
    ) -> Result<Option<NativeOutcome>, LedgerError> {
        Ok(self.native_engine()?.outcome(request)?)
    }
    /// The committed native prefix; pending candidates are never visible.
    pub fn native_core(&self) -> Result<&Core<NativeState>, LedgerError> {
        Ok(self.native_engine()?.committed_core()?)
    }
    pub fn native_sequence(&self) -> Result<SessionSeq, LedgerError> {
        Ok(self.native_engine()?.sequence()?)
    }
    pub fn native_read_at_least(
        &self,
        boundary: NativeReadBoundary,
    ) -> Result<&Core<NativeState>, LedgerError> {
        Ok(self
            .native_engine()?
            .read_at_least(boundary, &self.consensus.status())?)
    }
    /// Request a quorum read barrier for a native read; its boundary arrives
    /// in a later poll under the same correlation.
    pub fn native_read_index(&mut self, correlation: ReadCorrelation) -> Result<(), LedgerError> {
        self.check()?;
        if !self.is_authoritative() || self.retained.is_some() {
            return Err(LedgerError::NotReady {
                leader: self.status().leader_id,
            });
        }
        let engine = self
            .native
            .as_deref_mut()
            .ok_or(LedgerError::NativeUnsupported)?;
        engine.read_index(&mut self.consensus, correlation)?;
        Ok(())
    }
}
