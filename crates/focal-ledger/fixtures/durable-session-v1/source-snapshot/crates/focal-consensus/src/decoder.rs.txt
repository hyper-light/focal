//! An irreversible per-group decoder floor, fsynced before capability publication.
//! The existing WAL owner performs all physical writes; polling never blocks.
use super::*;
use focal_log::WalAppend;

const MAGIC: &[u8; 8] = b"FOCALDF1";
const ALLOWANCE: usize = 4096;

pub(super) struct PendingDecoderFloor {
    hash: [u8; 32],
    record: Record,
    receipt: Option<WalAppend>,
    _allocation: Allocation,
}
impl DurableNode {
    /// Register the actual compiled application decoder before replay or Raft
    /// participation. This does not persist or advertise a capability by itself.
    /// A group accepts one immutable fingerprint; changing it requires an
    /// explicit future versioned upgrade protocol rather than replacing this fact.
    pub fn confirm_decoder(&mut self, hash: [u8; 32]) -> Result<(), ConsensusError> {
        self.check_state()?;
        if self
            .required_decoder
            .is_some_and(|required| required != hash)
            || self
                .confirmed_decoder
                .is_some_and(|confirmed| confirmed != hash)
        {
            return Err(ConsensusError::DecoderMismatch);
        }
        self.confirmed_decoder = Some(hash);
        Ok(())
    }
    pub fn required_decoder(&self) -> Option<[u8; 32]> {
        self.required_decoder
    }
    /// Only this predicate authorizes advertising the local durable capability.
    pub fn decoder_floor_ready(&self, hash: [u8; 32]) -> bool {
        !self.failed
            && self.required_decoder == Some(hash)
            && self.confirmed_decoder == Some(hash)
            && self.decoder_write.is_none()
    }
    pub(super) fn decoder_confirmed(&self) -> bool {
        self.required_decoder.is_none() || self.required_decoder == self.confirmed_decoder
    }
    /// Stage an immutable requirement between existing Ready/checkpoint work.
    /// The bounded record is retained across queue pressure and caller loss.
    /// Call try_finish_decoder_floor or try_drain to enqueue/poll its fsync.
    pub fn begin_decoder_floor(&mut self, hash: [u8; 32]) -> Result<(), ConsensusError> {
        self.check()?;
        if self.confirmed_decoder != Some(hash) {
            return Err(ConsensusError::DecoderUnconfirmed);
        }
        if let Some(required) = self.required_decoder {
            return if required == hash {
                Ok(())
            } else {
                Err(ConsensusError::DecoderMismatch)
            };
        }
        if let Some(pending) = &self.decoder_write {
            return if pending.hash == hash {
                Ok(())
            } else {
                Err(ConsensusError::DecoderMismatch)
            };
        }
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
        let record = floor_record(self.config.group_id, hash)?;
        self.wal.validate_append(std::slice::from_ref(&record))?;
        self.decoder_write = Some(PendingDecoderFloor {
            hash,
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
            match self.wal.append_async_in(
                std::slice::from_ref(&pending.record),
                BudgetLane::Completion,
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
        self.required_decoder = Some(pending.hash);
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

#[cfg(test)]
#[path = "decoder_tests.rs"]
mod tests;
