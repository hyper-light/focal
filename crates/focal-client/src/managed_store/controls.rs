use super::*;

impl ManagedOperationStore {
    /// Lost replies retry this exact saved control; timeouts never clear it.
    pub fn pending_control(&self) -> Result<Option<RequestStreamControlInput>, ManagedStoreError> {
        let (_, state) = self.load()?;
        Ok(state.control)
    }
    /// Persist a bounded contiguous manifest of complete, locally synced results.
    /// Reading a result or dropping a process never calls this implicitly.
    pub fn prepare_acknowledgment(
        &self,
        id: RequestId,
        maximum: u32,
    ) -> Result<RequestStreamControlInput, ManagedStoreError> {
        let (directory, mut state) = self.load()?;
        control_ready(&state, id)?;
        if maximum == 0 || maximum > self.limits.window {
            return Err(ManagedStoreError::Capacity);
        }
        let mut receipts = Vec::new();
        receipts
            .try_reserve_exact(maximum as usize)
            .map_err(|_| ManagedStoreError::Capacity)?;
        let mut through = state.retired;
        for entry in state.entries.iter().take(maximum as usize) {
            let Some(receipt) = self.saved_receipt(&directory, entry)? else {
                break;
            };
            receipts.push(ManagedReceiptAck {
                key: receipt.key,
                receipt_hash: receipt
                    .content_hash()
                    .map_err(|_| ManagedStoreError::Corrupt)?,
            });
            through = receipt.key.ordinal;
        }
        if receipts.is_empty() {
            return Err(ManagedStoreError::Unresolved);
        }
        let command = RequestStreamCommand::Acknowledge {
            stream: state.stream()?,
            expected_revision: state.revision,
            through,
            receipts,
        };
        self.save_control(&directory, &mut state, id, command)
    }
    /// A reserved but never prepared gap receives a dedicated persisted seal
    /// intent. No domain command is fabricated to fill that gap.
    pub fn prepare_seal(
        &self,
        operation: ManagedOperationId,
        id: RequestId,
    ) -> Result<RequestStreamControlInput, ManagedStoreError> {
        let (directory, mut state) = self.load()?;
        control_ready(&state, id)?;
        let index = state.index(operation)?;
        let entry = state
            .entries
            .get_mut(index)
            .ok_or(ManagedStoreError::Corrupt)?;
        let binding = if let Some(binding) = entry.seal {
            binding
        } else if entry.prepared || directory.exists(&prepared_name(entry.key.ordinal))? {
            let prepared = self.prepared(
                &directory,
                entry.key,
                entry.intent.ok_or(ManagedStoreError::Corrupt)?,
            )?;
            let (_, family, intent_hash) = focal_wire::managed_request_identity(&prepared.request)
                .map_err(|_| ManagedStoreError::Corrupt)?;
            entry.prepared = true;
            SealBinding {
                family,
                intent_hash,
            }
        } else {
            let encoded = encode(&(entry.key, entry.intent), 256)?;
            let mut hash = blake3::Hasher::new_derive_key("focal.managed-unissued-seal.v1");
            hash.update(&encoded);
            SealBinding {
                family: ManagedRequestFamily::Domain,
                intent_hash: ContentHash(*hash.finalize().as_bytes()),
            }
        };
        entry.seal = Some(binding);
        let command = RequestStreamCommand::Seal {
            key: entry.key,
            expected_revision: state.revision,
            family: binding.family,
            intent_hash: binding.intent_hash,
        };
        self.save_control(&directory, &mut state, id, command)
    }
    /// Stops allocation durably. Restart and process exit cannot reverse this.
    pub fn stop_issuance(&self) -> Result<u64, ManagedStoreError> {
        let (directory, mut state) = self.load()?;
        if state.registered.is_none() {
            return Err(ManagedStoreError::NotRegistered);
        }
        state.stopped = true;
        self.save(&directory, &state)?;
        Ok(state.frontier)
    }
    pub fn prepare_close(
        &self,
        id: RequestId,
    ) -> Result<RequestStreamControlInput, ManagedStoreError> {
        let (directory, mut state) = self.load()?;
        control_ready(&state, id)?;
        if !state.stopped || state.retired != state.frontier {
            return Err(ManagedStoreError::Unresolved);
        }
        let command = RequestStreamCommand::Close {
            stream: state.stream()?,
            expected_revision: state.revision,
            issued_through: state.frontier,
        };
        self.save_control(&directory, &mut state, id, command)
    }
    /// Complete only the exact saved control. ACK retirement is written before
    /// deleting journal bodies; replayed old m1 IDs are then fenced by ordinal.
    pub fn record_control(
        &self,
        receipt: RequestStreamControlReceipt,
    ) -> Result<(), ManagedStoreError> {
        let (directory, mut state) = self.load()?;
        if state.last_control.as_ref() == Some(&receipt) {
            return Ok(());
        }
        let input = state
            .control
            .as_ref()
            .ok_or(ManagedStoreError::ReceiptMismatch)?;
        validate_control_receipt(input, &receipt)?;
        let prior_index = state
            .last_control
            .as_ref()
            .or(state.registered.as_ref())
            .map(|saved| saved.raft_index)
            .ok_or(ManagedStoreError::Corrupt)?;
        if receipt.raft_index <= prior_index {
            return Err(ManagedStoreError::ReceiptMismatch);
        }
        match (&input.command, &receipt.outcome) {
            (
                RequestStreamCommand::Acknowledge {
                    stream,
                    expected_revision,
                    through,
                    receipts,
                },
                RequestStreamControlOutcome::Acknowledged {
                    stream: actual,
                    revision,
                    through: actual_through,
                },
            ) => {
                let next_revision = state
                    .revision
                    .checked_add(1)
                    .ok_or(ManagedStoreError::Capacity)?;
                if stream != actual
                    || expected_revision != &state.revision
                    || *revision != next_revision
                    || through != actual_through
                    || *through <= state.retired
                    || *through > state.frontier
                {
                    return Err(ManagedStoreError::ReceiptMismatch);
                }
                let count = through
                    .checked_sub(state.retired)
                    .and_then(|n| usize::try_from(n).ok())
                    .ok_or(ManagedStoreError::Corrupt)?;
                if count != receipts.len() {
                    return Err(ManagedStoreError::Corrupt);
                }
                for (entry, expected) in state.entries.iter().take(count).zip(receipts) {
                    let saved = self
                        .saved_receipt(&directory, entry)?
                        .ok_or(ManagedStoreError::Corrupt)?;
                    if saved.key != expected.key
                        || saved
                            .content_hash()
                            .map_err(|_| ManagedStoreError::Corrupt)?
                            != expected.receipt_hash
                    {
                        return Err(ManagedStoreError::Corrupt);
                    }
                }
                if count > state.entries.len() {
                    return Err(ManagedStoreError::Corrupt);
                }
                state.entries.drain(..count);
                state.retired = *through;
                state.revision = next_revision;
            }
            (
                RequestStreamCommand::Seal {
                    key,
                    expected_revision,
                    family,
                    intent_hash,
                },
                RequestStreamControlOutcome::Sealed(outcome),
            ) => {
                let next_revision = state
                    .revision
                    .checked_add(1)
                    .ok_or(ManagedStoreError::Capacity)?;
                if *expected_revision != state.revision || outcome.raft_index > receipt.raft_index {
                    return Err(ManagedStoreError::ReceiptMismatch);
                }
                validate_bound_receipt(*key, *family, *intent_hash, outcome)?;
                let index = state.index(ManagedOperationId::from_key(*key)?)?;
                let entry = state
                    .entries
                    .get_mut(index)
                    .ok_or(ManagedStoreError::Corrupt)?;
                let hash = outcome
                    .content_hash()
                    .map_err(|_| ManagedStoreError::Corrupt)?;
                if entry.receipt.is_some_and(|saved| saved != hash) {
                    return Err(ManagedStoreError::ReceiptMismatch);
                }
                let name = receipt_name(key.ordinal);
                if directory.exists(&name)? {
                    let saved: ManagedReceipt =
                        decode(&directory.read(&name, RECEIPT_MAGIC, RECEIPT_BYTES)?)?;
                    if &saved != outcome.as_ref() {
                        return Err(ManagedStoreError::ReceiptMismatch);
                    }
                } else if entry.receipt.is_some() {
                    return Err(ManagedStoreError::Corrupt);
                } else {
                    directory.write(
                        &name,
                        RECEIPT_MAGIC,
                        &encode(outcome.as_ref(), RECEIPT_BYTES)?,
                        false,
                    )?;
                }
                entry.receipt = Some(hash);
                state.revision = next_revision;
            }
            (
                RequestStreamCommand::Close {
                    stream,
                    expected_revision,
                    issued_through,
                },
                RequestStreamControlOutcome::Closed {
                    stream: actual,
                    vacant_generation,
                },
            ) => {
                if stream != actual
                    || *vacant_generation != stream.generation
                    || !state.stopped
                    || *expected_revision != state.revision
                    || *issued_through != state.frontier
                    || state.retired != state.frontier
                {
                    return Err(ManagedStoreError::ReceiptMismatch);
                }
                state.closed = true;
            }
            _ => return Err(ManagedStoreError::ReceiptMismatch),
        }
        state.control = None;
        state.last_control = Some(receipt);
        self.save(&directory, &state)
    }
    fn save_control(
        &self,
        directory: &Directory,
        state: &mut State,
        id: RequestId,
        command: RequestStreamCommand,
    ) -> Result<RequestStreamControlInput, ManagedStoreError> {
        let input = RequestStreamControlInput {
            cluster: state.context.cluster,
            ledger: state.context.ledger,
            principal: state.context.principal,
            id,
            command,
        };
        state.control = Some(input.clone());
        self.save(directory, state)?;
        Ok(input)
    }
}

fn control_ready(state: &State, id: RequestId) -> Result<(), ManagedStoreError> {
    if id.is_zero() {
        return Err(ManagedStoreError::InvalidId);
    }
    if state.registered.is_none() {
        return Err(ManagedStoreError::NotRegistered);
    }
    if state.closed {
        return Err(ManagedStoreError::Retired);
    }
    if state.control.is_some() {
        return Err(ManagedStoreError::ControlPending);
    }
    if id == state.registration.id
        || state
            .last_control
            .as_ref()
            .is_some_and(|saved| saved.id == id)
    {
        return Err(ManagedStoreError::Conflict);
    }
    Ok(())
}
fn validate_control_receipt(
    input: &RequestStreamControlInput,
    receipt: &RequestStreamControlReceipt,
) -> Result<(), ManagedStoreError> {
    if receipt.cluster != input.cluster
        || receipt.ledger != input.ledger
        || receipt.principal != input.principal
        || receipt.id != input.id
        || receipt.raft_index == 0
        || receipt.intent_hash
            != input
                .intent_hash()
                .map_err(|_| ManagedStoreError::Corrupt)?
    {
        return Err(ManagedStoreError::ReceiptMismatch);
    }
    Ok(())
}
pub(super) fn validate_registration(
    state: &State,
    receipt: &RequestStreamControlReceipt,
) -> Result<(), ManagedStoreError> {
    validate_control_receipt(&state.registration, receipt)?;
    let RequestStreamCommand::Register { owner, window, .. } = state.registration.command else {
        return Err(ManagedStoreError::Corrupt);
    };
    match receipt.outcome {
        RequestStreamControlOutcome::Registered(RequestStreamState::Active {
            stream,
            owner: actual_owner,
            revision: 1,
            window: actual_window,
            acknowledged_through: 0,
        }) if stream == state.stream()? && owner == actual_owner && window == actual_window => {
            Ok(())
        }
        _ => Err(ManagedStoreError::ReceiptMismatch),
    }
}
