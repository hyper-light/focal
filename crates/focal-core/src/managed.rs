//! Scoped domain execution. The session's committed stream registry owns
//! admission, exact outcomes and retirement for both domain and cursor requests.
//! This module never substitutes a legacy request key or writes its receipt map.
use crate::access::Recorder;
use crate::overlay::RowVersion;
use crate::*;

/// Admission policy may evolve independently of the committed intent's rules.
/// Replay always carries the version decoded from the prepared contract.
#[derive(Clone, Copy)]
enum ManagedStage {
    AdmitV1,
    Replay(execution::Version),
}
impl ManagedStage {
    fn version(self) -> execution::Version {
        match self {
            Self::AdmitV1 => execution::Version::V1,
            Self::Replay(version) => version,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedManagedMutation {
    pub schema: u16,
    pub base: SessionSeq,
    pub input: ManagedAuthenticatedInput,
    pub command_hash: ContentHash,
    pub footprint: Footprint,
}

/// Local deterministic result, not a committed receipt. The ledger assigns the
/// actual Raft index and publishes its ManagedReceipt only after commitment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedApplyResult {
    pub key: ManagedRequestKey,
    pub sequence: SessionSeq,
    pub command_hash: ContentHash,
    pub outcome: CommandResult,
    pub deltas: Vec<Delta>,
    pub effects: Vec<EffectIntent>,
}

/// Owned row updates from the actual reducer, with immutable provenance. The
/// enclosing session must also validate its stream registry generation/floor;
/// those protocol controls intentionally do not advance the domain sequence.
#[derive(Debug)]
pub struct StagedManagedMutation {
    prepared: PreparedManagedMutation,
    result: ManagedApplyResult,
    version: RowVersion,
    basis: [u8; 32],
    seal: [u8; 32],
}
impl StagedManagedMutation {
    pub fn prepared(&self) -> &PreparedManagedMutation {
        &self.prepared
    }
    pub fn result(&self) -> &ManagedApplyResult {
        &self.result
    }
    pub fn patch(&self) -> RowPatch<'_> {
        RowPatch {
            ledger: self.prepared.input.key.stream.ledger,
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
        self.validate_basis(view, &core.limits)?;
        view.tail = Some(&self.version);
        view.sequence = self.version.sequence;
        Ok(view)
    }
    fn validate_basis(&self, view: CoreView<'_>, limits: &Limits) -> Result<(), CoreError> {
        execution::Version::from_schema(self.prepared.schema)?;
        let sequence = SessionSeq(view.sequence.0.checked_add(1).ok_or(CoreError::Exhausted)?);
        if self.prepared.base != view.sequence
            || self.prepared.input.key.stream.ledger != view.ledger()
            || self.result.key != self.prepared.input.key
            || self.result.sequence != sequence
            || self.version.sequence != sequence
            || self.result.command_hash != self.prepared.command_hash
            || !self.version.rows.epochs.is_empty()
            || !self.version.rows.receipts.is_empty()
            || self.basis != basis(view, limits)?
            || self.seal != output_seal(&self.prepared, &self.result, &self.version)?
        {
            return Err(CoreError::StalePreparation);
        }
        Ok(())
    }
}

impl PendingState {
    pub fn validate_managed_next(
        &self,
        core: &Core,
        staged: &StagedManagedMutation,
    ) -> Result<(), CoreError> {
        if self.rows.len() >= self.rows.capacity() {
            return Err(CoreError::Exhausted);
        }
        staged.validate_basis(self.view(core)?, &core.limits)
    }
    pub fn accept_managed(
        &mut self,
        core: &Core,
        staged: StagedManagedMutation,
    ) -> Result<(PreparedManagedMutation, ManagedApplyResult), CoreError> {
        self.validate_managed_next(core, &staged)?;
        self.accept_managed_row(core.state.ledger, staged.version);
        Ok((staged.prepared, staged.result))
    }
}

impl Core {
    /// Caller supplies an already admitted byte allowance. Only legacy request
    /// deduplication, epoch checks and legacy receipt insertion are bypassed.
    /// Namespace, actor, authority, command/revision, graph and resource checks
    /// use the same domain reducer as legacy requests.
    pub fn stage_managed_pending_bounded(
        &self,
        pending: &PendingState,
        input: &ManagedAuthenticatedInput,
        max_bytes: usize,
    ) -> Result<StagedManagedMutation, StagingError> {
        self.stage_managed_bounded(pending, input, max_bytes, ManagedStage::AdmitV1)
    }

    fn stage_managed_bounded(
        &self,
        pending: &PendingState,
        input: &ManagedAuthenticatedInput,
        max_bytes: usize,
        mode: ManagedStage,
    ) -> Result<StagedManagedMutation, StagingError> {
        let view = pending.view(self).map_err(|_| {
            StagingError::Domain(refuse(
                ErrorCode::RevisionConflict,
                "pending prefix changed",
            ))
        })?;
        let recorder = Recorder::bounded(self.state.ledger, view.sequence, max_bytes);
        let result = self.stage_managed_recorded(input, view, &recorder, mode);
        if recorder.failed() {
            Err(StagingError::Capacity)
        } else {
            result.map_err(StagingError::Domain)
        }
    }

    /// Recompute actual rows from a committed, versioned intent. Stream registry
    /// fencing is validated by the enclosing ledger before this replay boundary.
    pub fn replay_managed_bounded(
        &self,
        prepared: &PreparedManagedMutation,
        max_bytes: usize,
    ) -> Result<StagedManagedMutation, CoreError> {
        let version = execution::Version::from_schema(prepared.schema)?;
        if prepared.base != self.sequence() {
            return Err(CoreError::StalePreparation);
        }
        let staged = self
            .stage_managed_bounded(
                &PendingState::new(),
                &prepared.input,
                max_bytes,
                ManagedStage::Replay(version),
            )
            .map_err(staging_error)?;
        if staged.prepared != *prepared {
            return Err(CoreError::Checksum);
        }
        Ok(staged)
    }

    /// Independent re-execution audits the staged rows and result under a fresh
    /// bounded workspace. No handlers or external effects execute in the reducer.
    pub fn audit_managed_pending_stage(
        &self,
        pending: &PendingState,
        staged: &StagedManagedMutation,
        max_bytes: usize,
    ) -> Result<(), CoreError> {
        let view = pending.view(self)?;
        staged.validate_basis(view, &self.limits)?;
        let version = execution::Version::from_schema(staged.prepared.schema)?;
        let replay = self
            .stage_managed_bounded(
                pending,
                &staged.prepared.input,
                max_bytes,
                ManagedStage::Replay(version),
            )
            .map_err(staging_error)?;
        if staged.prepared != replay.prepared
            || staged.result != replay.result
            || staged.version != replay.version
            || staged.basis != replay.basis
            || staged.seal != replay.seal
        {
            return Err(CoreError::Checksum);
        }
        Ok(())
    }

    /// Validate before any graph/registry/Core publication begins. Row merging
    /// retains the existing budgeted map-allocation boundary of legacy Core.
    pub fn validate_managed(
        &self,
        staged: &StagedManagedMutation,
    ) -> Result<SessionSeq, CoreError> {
        staged.validate_basis(self.view(), &self.limits)?;
        Ok(staged.result.sequence)
    }
    pub fn publish_managed(
        &mut self,
        staged: StagedManagedMutation,
    ) -> Result<ManagedApplyResult, CoreError> {
        self.validate_managed(&staged)?;
        staged.version.rows.publish(&mut self.state);
        self.state.sequence = staged.result.sequence;
        Ok(staged.result)
    }

    fn stage_managed_recorded(
        &self,
        input: &ManagedAuthenticatedInput,
        view: CoreView<'_>,
        recorder: &Recorder,
        mode: ManagedStage,
    ) -> Result<StagedManagedMutation, DomainOutcome> {
        let rules = mode.version();
        let state = access::ReadState::overlay(&self.state, view.prior, view.sequence, recorder);
        if input.key.stream.ledger != state.ledger() {
            return Err(refuse(ErrorCode::InvalidNamespace, "wrong ledger"));
        }
        if !rules.managed_key_valid(&input.key) {
            return Err(refuse(
                ErrorCode::InvalidSchema,
                "invalid managed request identity",
            ));
        }
        if matches!(
            input.command,
            Command::NegotiateEpoch { .. } | Command::AdvanceEpochFloor { .. }
        ) {
            return Err(refuse(
                ErrorCode::InvalidEpoch,
                "managed requests cannot alter legacy epochs",
            ));
        }
        let bytes = rules
            .managed_input_size(input)
            .map_err(|_| refuse(ErrorCode::InvalidSchema, "managed command codec"))?;
        recorder.read(AccessKey::Limits);
        if bytes > self.limits.max_command_bytes {
            return Err(refuse(ErrorCode::Capacity, "managed command bytes"));
        }
        if !recorder.reserve_bytes(4096) || !recorder.reserve_value(input) {
            return Err(refuse(
                ErrorCode::Capacity,
                "managed staging workspace exhausted",
            ));
        }
        let command_hash = rules.managed_hash(input)?;
        if let Some(expected) = input.expected_revision {
            let id = rules.claim_id(&input.command).ok_or_else(|| {
                refuse(ErrorCode::RevisionConflict, "command has no claim revision")
            })?;
            if state
                .claims
                .get(&id)
                .map(|claim| claim.lifecycle().revision)
                != Some(expected)
            {
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
        let overlay = access::WriteState::overlay(&self.state, view.prior, sequence, recorder);
        if matches!(mode, ManagedStage::AdmitV1) {
            admission::validate_admission(
                &overlay,
                &self.limits,
                input.key.stream.principal,
                &input.authority,
                &input.command,
            )?;
        }
        let (mut draft, outcome, deltas, effects) =
            rules.execute_managed_on(overlay, &self.limits, input, sequence)?;
        let result = ManagedApplyResult {
            key: input.key,
            sequence,
            command_hash,
            outcome,
            deltas,
            effects,
        };
        if !recorder.reserve_value(&result) {
            return Err(refuse(
                ErrorCode::Capacity,
                "managed result workspace exhausted",
            ));
        }
        draft.set_sequence(sequence);
        let version = RowVersion {
            sequence,
            rows: draft.into_writes().ok_or_else(|| {
                refuse(ErrorCode::InvalidTransition, "missing managed row output")
            })?,
        };
        let prepared = PreparedManagedMutation {
            schema: rules.schema(),
            base: view.sequence,
            input: input.clone(),
            command_hash,
            footprint: Footprint {
                ledger: state.ledger(),
                session_exclusive: true,
            },
        };
        let basis = basis(view, &self.limits)
            .map_err(|_| refuse(ErrorCode::InvalidSchema, "managed view identity"))?;
        let seal = output_seal(&prepared, &result, &version)
            .map_err(|_| refuse(ErrorCode::InvalidSchema, "managed output identity"))?;
        Ok(StagedManagedMutation {
            prepared,
            result,
            version,
            basis,
            seal,
        })
    }
}

fn staging_error(error: StagingError) -> CoreError {
    match error {
        StagingError::Domain(error) => CoreError::Determinism(error),
        StagingError::Capacity => CoreError::Exhausted,
    }
}
fn basis(view: CoreView<'_>, limits: &Limits) -> Result<[u8; 32], CoreError> {
    crate::epoch::hash(&(&view, limits)).map_err(|_| CoreError::Checksum)
}
fn output_seal(
    prepared: &PreparedManagedMutation,
    result: &ManagedApplyResult,
    version: &RowVersion,
) -> Result<[u8; 32], CoreError> {
    crate::epoch::hash(&(prepared, result, version)).map_err(|_| CoreError::Checksum)
}

#[cfg(test)]
#[path = "managed_tests.rs"]
mod tests;
