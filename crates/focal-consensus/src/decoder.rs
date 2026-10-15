//! An irreversible per-group decoder floor and one explicitly registered successor.
//! The original promise and ordered transition survive every checkpoint. This is
//! local durable decoder readiness, not a replicated application activation gate.
//! The existing WAL owner performs all physical writes; polling never blocks.
use super::*;
use focal_log::WalAppend;

const MAGIC: &[u8; 8] = b"FOCALDF1";
const TRANSITION_MAGIC: &[u8; 8] = b"FOCALDT1";
const TRANSITION_SCHEMA: u16 = 1;
const TRANSITION_BYTES: usize = 74;
const ALLOWANCE: usize = 4096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct DecoderPair {
    pub(super) predecessor: [u8; 32],
    pub(super) successor: [u8; 32],
}
/// A write of the group's decoder records: its floor, or its one transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FloorWrite {
    Baseline([u8; 32]),
    Transition(DecoderPair),
}

/// What a group's records state of the decoders its entries need, and what its application
/// confirmed it compiled: the gate both backends keep ([27] §15.7), each making the records
/// durable its own way.
///
/// [27]: ../../../docs/archictecutre/27-consensus-roadmap-and-slates-port.md
#[derive(Debug, Default)]
pub(super) struct DecoderGate {
    /// The floor the group's records state durable.
    pub(super) required: Option<[u8; 32]>,
    /// The application's compiled decoder, confirmed.
    confirmed: Option<[u8; 32]>,
    /// The compiled ordered pair the application registered.
    compiled: Option<DecoderPair>,
    /// The transition the group's records state durable.
    pub(super) transition: Option<DecoderPair>,
}

impl DecoderGate {
    /// The gate of a group whose records state `required` and `transition`, before its
    /// application confirms anything.
    pub(super) fn new(required: Option<[u8; 32]>, transition: Option<DecoderPair>) -> Self {
        Self {
            required,
            confirmed: None,
            compiled: None,
            transition,
        }
    }
    /// The application's compiled decoder. A single hash cannot confirm recovery containing a
    /// decoder transition, even when that hash equals the effective successor requirement.
    pub(super) fn confirm(&mut self, hash: [u8; 32]) -> Result<(), ConsensusError> {
        if self.required.is_some_and(|required| required != hash)
            || self.confirmed.is_some_and(|confirmed| confirmed != hash)
            || self
                .transition
                .is_some_and(|pair| self.compiled != Some(pair))
        {
            return Err(ConsensusError::DecoderMismatch);
        }
        self.confirmed = Some(hash);
        Ok(())
    }
    /// The application's compiled ordered pair. A matching single predecessor registration may
    /// be widened once.
    pub(super) fn confirm_pair(
        &mut self,
        predecessor: [u8; 32],
        successor: [u8; 32],
    ) -> Result<(), ConsensusError> {
        let pair = DecoderPair {
            predecessor,
            successor,
        };
        if predecessor == successor
            || self.required.is_some_and(|hash| hash != predecessor)
            || self.confirmed.is_some_and(|hash| hash != predecessor)
            || self.compiled.is_some_and(|registered| registered != pair)
            || self.transition.is_some_and(|durable| durable != pair)
        {
            return Err(ConsensusError::DecoderMismatch);
        }
        self.compiled = Some(pair);
        self.confirmed = Some(predecessor);
        Ok(())
    }
    /// Effective minimum decoder: the transition's successor once it is durable, else the floor.
    pub(super) fn effective(&self) -> Option<[u8; 32]> {
        self.transition.map(|pair| pair.successor).or(self.required)
    }
    /// Whether the application confirmed what the records require.
    pub(super) fn confirmed(&self) -> bool {
        self.required
            .is_none_or(|hash| self.confirmed == Some(hash))
            && self
                .transition
                .is_none_or(|pair| self.compiled == Some(pair))
    }
    /// Whether the records state `hash` durable, as the floor or as the transition's successor.
    pub(super) fn states(&self, hash: [u8; 32]) -> bool {
        self.required == Some(hash) || self.transition.is_some_and(|pair| pair.successor == hash)
    }
    /// The floor's write `hash` asks for, with `pending` the write already staged: none when the
    /// records state it or it is staged. It remains idempotent after a transition; passing the
    /// successor here cannot bypass the separate transition record.
    pub(super) fn floor_write(
        &self,
        hash: [u8; 32],
        pending: Option<FloorWrite>,
    ) -> Result<Option<FloorWrite>, ConsensusError> {
        if self.confirmed != Some(hash) {
            return Err(ConsensusError::DecoderUnconfirmed);
        }
        if let Some(required) = self.required {
            return if required == hash {
                Ok(None)
            } else {
                Err(ConsensusError::DecoderMismatch)
            };
        }
        if let Some(pending) = pending {
            return if pending == FloorWrite::Baseline(hash) {
                Ok(None)
            } else {
                Err(ConsensusError::DecoderMismatch)
            };
        }
        Ok(Some(FloorWrite::Baseline(hash)))
    }
    /// The registered transition's write, once the predecessor floor is durable, with `pending`
    /// the write already staged: none when the records state it or it is staged.
    pub(super) fn transition_write(
        &self,
        pending: Option<FloorWrite>,
    ) -> Result<Option<FloorWrite>, ConsensusError> {
        let pair = self.compiled.ok_or(ConsensusError::DecoderUnconfirmed)?;
        if self.required != Some(pair.predecessor) {
            return Err(if pending.is_some() {
                ConsensusError::PersistencePending
            } else {
                ConsensusError::DecoderUnconfirmed
            });
        }
        if self.transition == Some(pair) {
            return Ok(None);
        }
        if let Some(pending) = pending {
            return if pending == FloorWrite::Transition(pair) {
                Ok(None)
            } else {
                Err(ConsensusError::DecoderMismatch)
            };
        }
        Ok(Some(FloorWrite::Transition(pair)))
    }
    /// The records state `intent`'s write durable.
    pub(super) fn written(&mut self, intent: FloorWrite) {
        match intent {
            FloorWrite::Baseline(hash) => self.required = Some(hash),
            FloorWrite::Transition(pair) => self.transition = Some(pair),
        }
    }
}

pub(super) struct PendingDecoderFloor {
    intent: FloorWrite,
    record: Record,
    receipt: Option<WalAppend>,
    _allocation: Allocation,
}
impl PendingDecoderFloor {
    /// Whether the log took the floor's write.
    pub(super) fn taken(&self) -> bool {
        self.receipt.is_some()
    }
}
impl LogNode {
    /// Register the actual compiled application decoder before replay or Raft
    /// participation. This never persists or advertises a capability. A single
    /// hash cannot confirm recovery containing a decoder transition, even when
    /// that hash equals the effective successor requirement.
    pub fn confirm_decoder(&mut self, hash: [u8; 32]) -> Result<(), ConsensusError> {
        self.check_state()?;
        self.decoders.confirm(hash)?;
        self.rebuild_recovered_membership()?;
        Ok(())
    }
    /// Trusted application composition registers exactly one compiled ordered
    /// pair. Both decoders must actually exist; this is not a client capability
    /// claim, a hash allowlist, or an application activation decision. A matching
    /// single predecessor registration may be explicitly widened once. Repeated
    /// registration preserves any pending write and its original receipt.
    pub fn confirm_decoder_pair(
        &mut self,
        predecessor: [u8; 32],
        successor: [u8; 32],
    ) -> Result<(), ConsensusError> {
        self.check_state()?;
        self.decoders.confirm_pair(predecessor, successor)?;
        self.rebuild_recovered_membership()?;
        Ok(())
    }
    /// Run the committed conf-change replay that a decoder-gated recovery had to
    /// defer past open (its unconfirmed decoder made `check` — and therefore
    /// `drain` — refuse). Called the instant confirmation completes, before any
    /// caller can step or campaign, so elections never see the stale
    /// snapshot-only voter set. The rebuilt events are retained for the caller's
    /// first drain, exactly as the non-gated constructor path retains them. Idempotent:
    /// once the flag is cleared, and while the decoder is not yet fully confirmed,
    /// it is a no-op.
    fn rebuild_recovered_membership(&mut self) -> Result<(), ConsensusError> {
        if !self.membership_rebuild_pending || !self.decoder_confirmed() {
            return Ok(());
        }
        let events = self.drain()?;
        self.recovered_events = Some(events);
        self.membership_rebuild_pending = false;
        Ok(())
    }
    /// Effective minimum decoder. This does not discard the original baseline
    /// promise, which remains required on recovery and in physical checkpoints.
    pub fn required_decoder(&self) -> Option<[u8; 32]> {
        self.decoders.effective()
    }
    /// Only this predicate authorizes advertising the local durable capability.
    /// After a transition, predecessor readiness also requires confirmation of
    /// the full compiled pair. A retained write suppresses publication until its
    /// exact durable fence is observed by this owner.
    pub fn decoder_floor_ready(&self, hash: [u8; 32]) -> bool {
        !self.failed
            && self.decoder_confirmed()
            && !self.membership_rebuild_pending
            && self.decoder_write.is_none()
            && self.decoders.states(hash)
    }
    pub(super) fn decoder_confirmed(&self) -> bool {
        self.decoders.confirmed()
    }
    /// Stage the original immutable requirement between existing Ready/checkpoint
    /// work. It remains idempotent after a transition; passing the successor here
    /// cannot bypass the separate transition record.
    pub fn begin_decoder_floor(&mut self, hash: [u8; 32]) -> Result<(), ConsensusError> {
        self.check()?;
        let pending = self.decoder_write.as_ref().map(|pending| pending.intent);
        match self.decoders.floor_write(hash, pending)? {
            Some(intent) => self.stage_decoder_write(intent),
            None => Ok(()),
        }
    }
    /// Stage the one registered transition after the predecessor floor is durable.
    /// Registration alone never authorizes successor publication. Call the same
    /// finish/try_finish_decoder_floor or try_drain methods to retain and observe
    /// this write through queue pressure, caller loss, and the actual fsync fence.
    pub fn begin_decoder_transition(&mut self) -> Result<(), ConsensusError> {
        self.check()?;
        let pending = self.decoder_write.as_ref().map(|pending| pending.intent);
        match self.decoders.transition_write(pending)? {
            Some(intent) => self.stage_decoder_write(intent),
            None => Ok(()),
        }
    }
    fn stage_decoder_write(&mut self, intent: FloorWrite) -> Result<(), ConsensusError> {
        if self.persistence_pending()
            || self.raw.has_ready()
            || self.recovered_events.is_some()
            || self.recovered_snapshot.is_some()
        {
            return Err(ConsensusError::PersistencePending);
        }
        let allocation = memory::reserve(
            &self.budget,
            BudgetKind::Control,
            BudgetLane::Completion,
            ALLOWANCE,
        )?;
        let record = match intent {
            FloorWrite::Baseline(hash) => floor_record(self.config.group_id, hash)?,
            FloorWrite::Transition(pair) => transition_record(self.config.group_id, pair)?,
        };
        self.wal.validate_append(std::slice::from_ref(&record))?;
        self.decoder_write = Some(PendingDecoderFloor {
            intent,
            record,
            receipt: None,
            _allocation: allocation,
        });
        Ok(())
    }
    pub fn try_finish_decoder_floor(&mut self) -> Result<bool, ConsensusError> {
        self.poll_decoder_floor(false)
    }
    /// Blocking-owner compatibility path over the same exact receipt. Async
    /// hosts use polling; no runtime blocking pool or auxiliary worker is added.
    pub fn finish_decoder_floor(&mut self) -> Result<(), ConsensusError> {
        if self.poll_decoder_floor(true)? {
            Ok(())
        } else {
            Err(ConsensusError::PersistencePending)
        }
    }
    pub(super) fn poll_decoder_floor(&mut self, blocking: bool) -> Result<bool, ConsensusError> {
        self.check()?;
        let Some(mut pending) = self.decoder_write.take() else {
            return Ok(true);
        };
        if self.persistence.is_some() || self.checkpoint.is_some() {
            self.decoder_write = Some(pending);
            return Err(ConsensusError::PersistencePending);
        }
        if pending.receipt.is_none() {
            let persisted = self.persisted.as_ref().map(|signal| signal());
            match self.wal.append_async_notified(
                std::slice::from_ref(&pending.record),
                BudgetLane::Completion,
                persisted,
            ) {
                Ok(receipt) => pending.receipt = Some(receipt),
                Err(LogError::Capacity) => {
                    self.decoder_write = Some(pending);
                    return Ok(false);
                }
                Err(error) => {
                    self.failed = true;
                    return Err(error.into());
                }
            }
            // Even an immediately finished writer is observed on the next owner
            // turn; initiating work cannot accidentally advertise before polling.
            if !blocking {
                self.decoder_write = Some(pending);
                return Ok(false);
            }
        }
        let receipt = pending.receipt.as_mut().ok_or(ConsensusError::Failed)?;
        let completed = if blocking {
            Some(receipt.wait_blocking())
        } else {
            receipt.try_complete()
        };
        let Some(completed) = completed else {
            self.decoder_write = Some(pending);
            return Ok(false);
        };
        if let Err(error) = completed {
            self.failed = true;
            return Err(error.into());
        }
        self.decoders.written(pending.intent);
        Ok(true)
    }
}
pub(super) fn floor_record(group: [u8; 16], hash: [u8; 32]) -> Result<Record, ConsensusError> {
    let mut payload = Vec::new();
    payload
        .try_reserve_exact(40)
        .map_err(|_| ConsensusError::Capacity)?;
    payload.extend_from_slice(MAGIC);
    payload.extend_from_slice(&hash);
    Ok(Record {
        log: LogicalLogId(group),
        kind: RecordKind::DecoderFloor,
        index: 0,
        term: 0,
        payload,
    })
}
pub(super) fn decode_floor(record: &Record) -> Result<[u8; 32], ConsensusError> {
    if record.index != 0
        || record.term != 0
        || record.payload.len() != 40
        || record.payload.get(..8) != Some(MAGIC.as_slice())
    {
        return Err(ConsensusError::Corruption("invalid decoder floor envelope"));
    }
    record
        .payload
        .get(8..)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(ConsensusError::Corruption(
            "invalid decoder floor fingerprint",
        ))
}

pub(super) fn transition_record(
    group: [u8; 16],
    pair: DecoderPair,
) -> Result<Record, ConsensusError> {
    if pair.predecessor == pair.successor {
        return Err(ConsensusError::DecoderMismatch);
    }
    let mut payload = Vec::new();
    payload
        .try_reserve_exact(TRANSITION_BYTES)
        .map_err(|_| ConsensusError::Capacity)?;
    payload.extend_from_slice(TRANSITION_MAGIC);
    payload.extend_from_slice(&TRANSITION_SCHEMA.to_be_bytes());
    payload.extend_from_slice(&pair.predecessor);
    payload.extend_from_slice(&pair.successor);
    Ok(Record {
        log: LogicalLogId(group),
        kind: RecordKind::DecoderTransition,
        index: 0,
        term: 0,
        payload,
    })
}
pub(super) fn decode_transition(record: &Record) -> Result<DecoderPair, ConsensusError> {
    if record.index != 0
        || record.term != 0
        || record.payload.len() != TRANSITION_BYTES
        || record.payload.get(..8) != Some(TRANSITION_MAGIC.as_slice())
        || record.payload.get(8..10) != Some(TRANSITION_SCHEMA.to_be_bytes().as_slice())
    {
        return Err(ConsensusError::Corruption(
            "invalid decoder transition envelope",
        ));
    }
    let predecessor = record
        .payload
        .get(10..42)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(ConsensusError::Corruption(
            "invalid decoder transition predecessor",
        ))?;
    let successor = record
        .payload
        .get(42..74)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(ConsensusError::Corruption(
            "invalid decoder transition successor",
        ))?;
    if predecessor == successor {
        return Err(ConsensusError::Corruption(
            "decoder transition has identical endpoints",
        ));
    }
    Ok(DecoderPair {
        predecessor,
        successor,
    })
}

#[cfg(test)]
#[path = "decoder_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "decoder_transition_tests.rs"]
mod transition_tests;
