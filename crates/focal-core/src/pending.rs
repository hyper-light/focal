//! Owned pending row versions. No admission candidate contains a whole State.
use crate::access::Recorder;
use crate::overlay::{RowVersion, RowWrites};
use crate::*;
use serde::ser::{SerializeMap, SerializeStruct};
use std::ops::Bound::{Excluded, Unbounded};

#[derive(Debug, Default)]
pub struct PendingState {
    ledger: Option<LedgerId>,
    pub(crate) rows: Vec<Option<RowVersion>>,
}
#[derive(Debug)]
pub struct StagedMutation {
    pub(crate) prepared: PreparedMutation,
    pub(crate) result: ApplyResult,
    pub(crate) version: RowVersion,
    basis: Option<[u8; 32]>,
}
#[derive(Debug)]
pub enum StagingError {
    Domain(DomainOutcome),
    Capacity,
}
#[derive(Clone, Copy)]
pub struct CoreView<'a> {
    pub(crate) state: &'a State,
    pub(crate) prior: &'a [Option<RowVersion>],
    pub(crate) tail: Option<&'a RowVersion>,
    pub(crate) sequence: SessionSeq,
}
#[derive(Clone, Copy, Serialize)]
pub struct RowPatch<'a> {
    pub(crate) ledger: LedgerId,
    pub(crate) base: SessionSeq,
    pub(crate) version: &'a RowVersion,
}
impl PendingState {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn len(&self) -> usize {
        self.rows.len()
    }
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
    pub fn slot_bytes() -> usize {
        std::mem::size_of::<Option<RowVersion>>()
    }
    pub fn reserve(&mut self, maximum: usize) -> Result<(), CoreError> {
        let additional = maximum
            .checked_sub(self.rows.len())
            .ok_or(CoreError::Exhausted)?;
        self.rows
            .try_reserve_exact(additional)
            .map_err(|_| CoreError::Exhausted)
    }
    pub fn matches_epoch(&self, output: &EpochOutput) -> bool {
        output.matches_pending_rows(&self.rows)
    }
    pub fn clear(&mut self) {
        self.rows.clear();
        self.ledger = None;
    }
    pub fn release_empty(&mut self) {
        if self.rows.is_empty() {
            self.rows = Vec::new();
        }
    }
    pub fn view<'a>(&'a self, core: &'a Core) -> Result<CoreView<'a>, CoreError> {
        if self
            .ledger
            .is_some_and(|ledger| ledger != core.state.ledger)
        {
            return Err(CoreError::Checksum);
        }
        let mut sequence = core.sequence();
        for row in &self.rows {
            sequence = SessionSeq(sequence.0.checked_add(1).ok_or(CoreError::Exhausted)?);
            if row.as_ref().is_none_or(|row| row.sequence != sequence) {
                return Err(CoreError::StalePreparation);
            }
        }
        Ok(CoreView {
            state: &core.state,
            prior: &self.rows,
            tail: None,
            sequence,
        })
    }
    /// Check provenance and capacity before the host submits the Raft proposal.
    pub fn validate_next(&self, core: &Core, staged: &StagedMutation) -> Result<(), CoreError> {
        let view = self.view(core)?;
        if self.rows.len() >= self.rows.capacity()
            || staged.prepared.base != view.sequence
            || staged.prepared.input.ledger != core.state.ledger
            || staged.basis != Some(basis(view, &core.limits)?)
        {
            return Err(CoreError::StalePreparation);
        }
        Ok(())
    }
    /// Capacity must already be reserved. Moving the row version allocates no memory.
    pub fn accept(
        &mut self,
        core: &Core,
        staged: StagedMutation,
    ) -> Result<(PreparedMutation, ApplyResult), CoreError> {
        self.validate_next(core, &staged)?;
        self.ledger = Some(core.state.ledger);
        self.rows.push(Some(staged.version));
        Ok((staged.prepared, staged.result))
    }
    /// Drop versions already included in the newly published committed core.
    pub fn drop_prefix(&mut self, count: usize, core: &Core) -> Result<(), CoreError> {
        if count == 0 {
            self.view(core)?;
            return Ok(());
        }
        let last = count
            .checked_sub(1)
            .and_then(|index| self.rows.get(index))
            .and_then(Option::as_ref)
            .ok_or(CoreError::StalePreparation)?;
        if last.sequence != core.sequence() {
            return Err(CoreError::StalePreparation);
        }
        self.rows.drain(..count);
        if self.rows.is_empty() {
            self.ledger = None;
        }
        self.view(core)?;
        Ok(())
    }
}
impl StagedMutation {
    pub fn prepared(&self) -> &PreparedMutation {
        &self.prepared
    }
    pub fn result(&self) -> &ApplyResult {
        &self.result
    }
    pub fn patch(&self) -> RowPatch<'_> {
        RowPatch {
            ledger: self.prepared.input.ledger,
            base: self.prepared.base,
            version: &self.version,
        }
    }
    pub fn view_after<'a>(
        &'a self,
        core: &'a Core,
        pending: &'a PendingState,
    ) -> Result<CoreView<'a>, CoreError> {
        let mut view = pending.view(core)?;
        if self.prepared.base != view.sequence || self.basis != Some(basis(view, &core.limits)?) {
            return Err(CoreError::StalePreparation);
        }
        view.tail = Some(&self.version);
        view.sequence = self.version.sequence;
        Ok(view)
    }
}
impl Core {
    pub fn view(&self) -> CoreView<'_> {
        CoreView {
            state: &self.state,
            prior: &[],
            tail: None,
            sequence: self.sequence(),
        }
    }
    pub fn stage_pending(
        &self,
        pending: &PendingState,
        input: &AuthenticatedInput,
    ) -> Result<StagedMutation, DomainOutcome> {
        let view = pending
            .view(self)
            .map_err(|_| refuse(ErrorCode::RevisionConflict, "pending prefix changed"))?;
        let recorder = Recorder::disabled(self.state.ledger, view.sequence);
        self.stage_recorded(input, view.prior, view.sequence, &recorder, true)
    }
    /// The caller owns this byte allowance before entry. All staging allocations
    /// consume it before row copies, input copies, graph scratch or output growth.
    pub fn stage_pending_bounded(
        &self,
        pending: &PendingState,
        input: &AuthenticatedInput,
        max_bytes: usize,
    ) -> Result<StagedMutation, StagingError> {
        let view = pending.view(self).map_err(|_| {
            StagingError::Domain(refuse(
                ErrorCode::RevisionConflict,
                "pending prefix changed",
            ))
        })?;
        let recorder = Recorder::bounded(self.state.ledger, view.sequence, max_bytes);
        let result = self.stage_recorded(input, view.prior, view.sequence, &recorder, true);
        if recorder.failed() {
            Err(StagingError::Capacity)
        } else {
            result.map_err(StagingError::Domain)
        }
    }
}
fn basis(view: CoreView<'_>, limits: &Limits) -> Result<[u8; 32], CoreError> {
    crate::epoch::hash(&(&view, limits)).map_err(|_| CoreError::Checksum)
}
impl RowPatch<'_> {
    pub fn ledger(&self) -> LedgerId {
        self.ledger
    }
    pub fn base(&self) -> SessionSeq {
        self.base
    }
    pub fn sequence(&self) -> SessionSeq {
        self.version.sequence
    }
}
macro_rules! rows {
    ($field:ident, $lookup:ident, $key:ty, $value:ty) => {
        impl<'a> CoreView<'a> {
            pub fn $lookup(&self, key: &$key) -> Option<&'a $value> {
                if let Some(value) = self.tail.and_then(|tail| tail.rows.$field.get(key)) {
                    return Some(value);
                }
                self.prior
                    .iter()
                    .rev()
                    .flatten()
                    .find_map(|version| version.rows.$field.get(key))
                    .or_else(|| self.state.$field.get(key))
            }
        }
        impl RowPatch<'_> {
            pub fn $field(&self) -> impl Iterator<Item = (&$key, &$value)> {
                self.version.rows.$field.iter()
            }
        }
    };
}
rows!(claims, claim, ClaimId, Claim);
rows!(validations, validation, ValidationId, Validation);
rows!(artifacts, artifact, ArtifactId, Artifact);
rows!(testaments, testament, TestamentId, Testament);
rows!(evidence_sets, evidence_set, EvidenceSetId, EvidenceSet);
rows!(runs, run, ValidationRunId, ValidationRun);
rows!(monitors, monitor, MonitorId, Monitor);
rows!(identities, identity, (ObjectKind, ContentHash), ObjectId);
rows!(epochs, epoch, ParticipantId, EpochWindow);
rows!(receipts, receipt, RequestKey, MutationReceipt);
impl CoreView<'_> {
    pub fn ledger(&self) -> LedgerId {
        self.state.ledger
    }
    pub fn sequence(&self) -> SessionSeq {
        self.sequence
    }
}
struct MapView<'a, K, V> {
    base: &'a BTreeMap<K, V>,
    prior: &'a [Option<RowVersion>],
    tail: Option<&'a RowVersion>,
    select: fn(&RowWrites) -> &BTreeMap<K, V>,
    table: StateTable,
}
impl<K: Ord + Copy + Serialize, V: Serialize> Serialize for MapView<'_, K, V> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let versions = || self.prior.iter().flatten().chain(self.tail);
        let count = versions()
            .try_fold(self.base.len(), |count, version| {
                count.checked_add(version.rows.added.get(&self.table).copied().unwrap_or(0))
            })
            .ok_or_else(|| serde::ser::Error::custom("view row count overflow"))?;
        let mut map = serializer.serialize_map(Some(count))?;
        let mut after = None;
        loop {
            let first = |rows: &BTreeMap<K, V>| {
                rows.range((after.map_or(Unbounded, Excluded), Unbounded))
                    .next()
                    .map(|(key, _)| *key)
            };
            let key = std::iter::once(first(self.base))
                .chain(versions().map(|version| first((self.select)(&version.rows))))
                .flatten()
                .min();
            let Some(key) = key else {
                break;
            };
            let value = self
                .tail
                .and_then(|tail| (self.select)(&tail.rows).get(&key))
                .or_else(|| {
                    self.prior
                        .iter()
                        .rev()
                        .flatten()
                        .find_map(|version| (self.select)(&version.rows).get(&key))
                })
                .or_else(|| self.base.get(&key))
                .ok_or_else(|| serde::ser::Error::custom("view row missing"))?;
            map.serialize_entry(&key, value)?;
            after = Some(key);
        }
        map.end()
    }
}
impl Serialize for CoreView<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("State", 12)?;
        state.serialize_field("ledger", &self.state.ledger)?;
        state.serialize_field("sequence", &self.sequence)?;
        macro_rules! field {
            ($field:ident, $table:ident) => {
                state.serialize_field(
                    stringify!($field),
                    &MapView {
                        base: &self.state.$field,
                        prior: self.prior,
                        tail: self.tail,
                        select: |rows| &rows.$field,
                        table: StateTable::$table,
                    },
                )?;
            };
        }
        field!(claims, Claims);
        field!(validations, Validations);
        field!(artifacts, Artifacts);
        field!(testaments, Testaments);
        field!(evidence_sets, EvidenceSets);
        field!(runs, Runs);
        field!(monitors, Monitors);
        field!(identities, Identities);
        field!(epochs, Epochs);
        field!(receipts, Receipts);
        state.end()
    }
}

impl Core {
    pub(crate) fn stage_recorded(
        &self,
        input: &AuthenticatedInput,
        prior: &[Option<RowVersion>],
        prefix: SessionSeq,
        recorder: &Recorder,
        stamp: bool,
    ) -> Result<StagedMutation, DomainOutcome> {
        let state = access::ReadState::overlay(&self.state, prior, prefix, recorder);
        if input.ledger != state.ledger() {
            return Err(refuse(ErrorCode::InvalidNamespace, "wrong ledger"));
        }
        if input.principal.is_zero() || input.request_id.is_zero() {
            return Err(refuse(
                ErrorCode::InvalidSchema,
                "zero principal or request ID",
            ));
        }
        let bytes = postcard::experimental::serialized_size(input)
            .map_err(|_| refuse(ErrorCode::InvalidSchema, "command codec"))?;
        recorder.read(AccessKey::Limits);
        if bytes > self.limits.max_command_bytes {
            return Err(refuse(ErrorCode::Capacity, "command bytes"));
        }
        if !recorder.reserve_bytes(4096) || !recorder.reserve_value(input) {
            return Err(refuse(
                ErrorCode::Capacity,
                "staging input workspace exhausted",
            ));
        }
        let hash = command_hash(input)
            .map_err(|_| refuse(ErrorCode::InvalidSchema, "command identity"))?;
        let key = RequestKey {
            principal: input.principal,
            epoch: input.request_epoch,
            id: input.request_id,
        };
        if let Some(receipt) = state.receipts.get(&key) {
            return if receipt.command_hash == hash {
                Err(DomainOutcome::Duplicate(Box::new(receipt.clone())))
            } else {
                Err(refuse(
                    ErrorCode::IdempotencyConflict,
                    "request key is bound to another command",
                ))
            };
        }
        if !matches!(input.command, Command::NegotiateEpoch { .. }) {
            let window = state.epochs.get(&input.principal).ok_or_else(|| {
                refuse(
                    ErrorCode::RequestEpochNotAdmitted,
                    "negotiate request generation",
                )
            })?;
            if input.request_epoch < window.minimum {
                return Err(refuse(
                    ErrorCode::RequestHistoryExpired,
                    "request generation expired",
                ));
            }
            if !window.admitted.contains(&input.request_epoch) {
                return Err(refuse(
                    ErrorCode::RequestEpochNotAdmitted,
                    "unallocated request generation",
                ));
            }
        }
        if state.receipts.len() >= self.limits.max_requests {
            return Err(refuse(
                ErrorCode::Capacity,
                "request receipt budget exhausted",
            ));
        }
        if let Some(expected) = input.expected_revision {
            let id = input.command.claim_id().ok_or_else(|| {
                refuse(ErrorCode::RevisionConflict, "command has no claim revision")
            })?;
            if state.claims.get(&id).map(|c| c.lifecycle().revision) != Some(expected) {
                return Err(refuse(
                    ErrorCode::RevisionConflict,
                    "claim revision changed",
                ));
            }
        }
        let sequence = SessionSeq(
            state
                .sequence()
                .0
                .checked_add(1)
                .ok_or_else(|| refuse(ErrorCode::Capacity, "session sequence exhausted"))?,
        );
        let overlay = access::WriteState::overlay(&self.state, prior, sequence, recorder);
        let (mut draft, outcome, deltas, effects) =
            reduce::execute_on(overlay, &self.limits, input, sequence)?;
        let receipt = MutationReceipt {
            ledger: state.ledger(),
            key,
            sequence,
            command_hash: hash,
            outcome,
        };
        if !recorder.reserve_value(&receipt) {
            return Err(refuse(
                ErrorCode::Capacity,
                "staging receipt workspace exhausted",
            ));
        }
        draft.receipts.insert(key, receipt.clone());
        draft.set_sequence(sequence);
        let version = RowVersion {
            sequence,
            rows: draft
                .into_writes()
                .ok_or_else(|| refuse(ErrorCode::InvalidTransition, "missing row output"))?,
        };
        let prepared = PreparedMutation {
            schema: SCHEMA_MAJOR,
            base: state.sequence(),
            input: input.clone(),
            command_hash: hash,
            footprint: Footprint {
                ledger: state.ledger(),
                session_exclusive: true,
            },
        };
        let basis = if stamp {
            Some(
                basis(
                    CoreView {
                        state: &self.state,
                        prior,
                        tail: None,
                        sequence: prefix,
                    },
                    &self.limits,
                )
                .map_err(|_| refuse(ErrorCode::InvalidSchema, "view identity"))?,
            )
        } else {
            None
        };
        Ok(StagedMutation {
            prepared,
            result: ApplyResult {
                receipt,
                deltas,
                effects,
            },
            version,
            basis,
        })
    }
}

#[cfg(test)]
#[path = "pending_tests.rs"]
mod tests;
