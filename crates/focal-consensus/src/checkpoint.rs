//! Nonblocking application checkpoint preparation and durable publication.
use super::*;
use focal_log::WalAppend;
use storage::PreparedSnapshot;

pub(super) struct PendingCheckpoint {
    prepared: PreparedSnapshot,
    records: Vec<Record>,
    receipt: Option<WalAppend>,
    // All pending payloads precede their staging permit in drop order.
    _allocation: Allocation,
}

impl DurableNode {
    pub fn checkpoint_pending(&self) -> bool {
        self.checkpoint.is_some()
    }

    /// Prepare an exact published-prefix checkpoint without waiting for disk.
    /// No RawNode mutation is allowed until its ticket completes or this still
    /// unadmitted checkpoint is explicitly canceled.
    pub fn begin_checkpoint(&mut self, index: u64, data: Vec<u8>) -> Result<(), ConsensusError> {
        self.check()?;
        if self.persistence_pending() {
            return Err(ConsensusError::PersistencePending);
        }
        if index == 0
            || index != self.delivered_index
            || index > self.raw.store().hard_state.commit
            || self.raw.has_ready()
        {
            return Err(ConsensusError::CheckpointIndex);
        }
        if data.len() > 8 * 1024 * 1024 {
            return Err(ConsensusError::Capacity);
        }
        let bytes = memory::staging_bytes(&self.raw, &self.config, data.capacity(), 0)?;
        let allocation = memory::reserve(
            &self.budget,
            BudgetKind::Pending,
            BudgetLane::Completion,
            bytes,
        )?;
        let result = catch_unwind(AssertUnwindSafe(|| {
            let term = self.raw.store().term(index)?;
            let mut snapshot = Snapshot::default();
            snapshot.mut_metadata().index = index;
            snapshot.mut_metadata().term = term;
            snapshot
                .mut_metadata()
                .set_conf_state(self.raw.store().conf_state.clone());
            snapshot.data = data;
            let prepared = self.raw.store().prepare_snapshot(&snapshot)?;
            let mut records = Vec::new();
            let count = self
                .raw
                .store()
                .entries
                .iter()
                .filter(|entry| entry.index > index)
                .count()
                .checked_add(3)
                .ok_or(ConsensusError::Capacity)?;
            records
                .try_reserve_exact(count)
                .map_err(|_| ConsensusError::Capacity)?;
            records.push(identity_record(&self.config)?);
            records.push(proto_record(
                self.config.group_id,
                RecordKind::Snapshot,
                index,
                term,
                &snapshot,
            )?);
            for entry in self
                .raw
                .store()
                .entries
                .iter()
                .filter(|entry| entry.index > index)
            {
                records.push(proto_record(
                    self.config.group_id,
                    RecordKind::Entry,
                    entry.index,
                    entry.term,
                    entry,
                )?);
            }
            let hard = &self.raw.store().hard_state;
            records.push(proto_record(
                self.config.group_id,
                RecordKind::HardState,
                hard.commit,
                hard.term,
                hard,
            )?);
            // Permanent wire/static-budget refusal must happen before entering
            // the retryable owner state; queue/free-memory pressure can retry.
            self.wal.validate_append(&records)?;
            Ok::<_, ConsensusError>(PendingCheckpoint {
                prepared,
                records,
                receipt: None,
                _allocation: allocation,
            })
        }));
        match result {
            Ok(Ok(pending)) => {
                self.checkpoint = Some(Box::new(pending));
                Ok(())
            }
            Ok(Err(error)) => Err(error),
            Err(_) => {
                self.failed = true;
                Err(ConsensusError::DependencyFailure)
            }
        }
    }
    /// Interest may be canceled before admission. An admitted rewrite must
    /// still reach its exact durable fence before this RawNode becomes mutable.
    pub fn cancel_unadmitted_checkpoint(&mut self) -> bool {
        if self
            .checkpoint
            .as_ref()
            .is_some_and(|pending| pending.receipt.is_none())
        {
            self.checkpoint = None;
            true
        } else {
            false
        }
    }
    pub fn try_finish_checkpoint(&mut self) -> Result<bool, ConsensusError> {
        self.poll_checkpoint(false)
    }
    pub fn finish_checkpoint(&mut self) -> Result<(), ConsensusError> {
        if self.poll_checkpoint(true)? {
            Ok(())
        } else {
            Err(ConsensusError::PersistencePending)
        }
    }
    fn poll_checkpoint(&mut self, blocking: bool) -> Result<bool, ConsensusError> {
        self.check()?;
        if self.checkpoint.is_none() {
            return Ok(true);
        }
        let result = catch_unwind(AssertUnwindSafe(|| self.checkpoint_progress(blocking)));
        match result {
            Ok(Ok(done)) => Ok(done),
            Ok(Err(error)) => {
                self.failed = true;
                Err(error)
            }
            Err(_) => {
                self.failed = true;
                Err(ConsensusError::DependencyFailure)
            }
        }
    }
    fn checkpoint_progress(&mut self, blocking: bool) -> Result<bool, ConsensusError> {
        let mut pending = self.checkpoint.take().ok_or(ConsensusError::Failed)?;
        if pending.receipt.is_none() {
            match self
                .wal
                .rewrite_checkpoint_async_in(&pending.records, BudgetLane::Completion)
            {
                Ok(receipt) => pending.receipt = Some(receipt),
                Err(focal_log::LogError::Capacity) if !blocking => {
                    self.checkpoint = Some(pending);
                    return Ok(false);
                }
                Err(error) => return Err(error.into()),
            }
        }
        let receipt = pending.receipt.as_mut().ok_or(ConsensusError::Failed)?;
        let completed = if blocking {
            Some(receipt.wait_blocking())
        } else {
            receipt.try_complete()
        };
        let Some(completed) = completed else {
            self.checkpoint = Some(pending);
            return Ok(false);
        };
        completed?;
        let PendingCheckpoint {
            prepared,
            records,
            receipt,
            mut _allocation,
        } = *pending;
        drop(records);
        drop(receipt);
        self.raw.mut_store().compact_prepared(prepared)?;
        _allocation
            .shrink_to(memory::raw_bytes(&self.raw)?)
            .map_err(|_| ConsensusError::Capacity)?;
        self.raw_allocation = Some(_allocation);
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_mutability_gate_can_cancel_only_before_writer_admission() {
        let directory = tempfile::tempdir().unwrap();
        let mut node =
            DurableNode::open(NodeConfig::single(1, [148; 16], [1; 16]), directory.path()).unwrap();
        node.campaign().unwrap();
        node.drain().unwrap();
        node.propose(b"before-checkpoint".to_vec()).unwrap();
        let index = node.drain().unwrap().applied_index;
        node.begin_checkpoint(index, b"complete-prefix".to_vec())
            .unwrap();
        assert!(node.persistence_pending());
        assert!(matches!(
            node.propose(vec![1]),
            Err(ConsensusError::PersistencePending)
        ));
        assert!(matches!(
            node.tick(),
            Err(ConsensusError::PersistencePending)
        ));
        assert!(matches!(
            node.try_drain(),
            Err(ConsensusError::PersistencePending)
        ));
        assert!(node.cancel_unadmitted_checkpoint());
        assert!(!node.persistence_pending());
        node.propose(b"after-cancel".to_vec()).unwrap();
        assert_eq!(node.drain().unwrap().committed[0].data, b"after-cancel");
    }

    #[test]
    fn async_checkpoint_failure_never_releases_success_and_recovery_uses_exact_generation_fence() {
        for point in [FaultPoint::AfterDataSync, FaultPoint::AfterFenceInstall] {
            let directory = tempfile::tempdir().unwrap();
            let config = NodeConfig::single(1, [148; 16], [2; 16]);
            let mut node = DurableNode::open(config.clone(), directory.path()).unwrap();
            node.campaign().unwrap();
            node.drain().unwrap();
            node.propose(b"retained-published-entry".to_vec()).unwrap();
            let index = node.drain().unwrap().applied_index;
            node.inject_fault_once(point);
            node.begin_checkpoint(index, b"durable-application-prefix".to_vec())
                .unwrap();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            loop {
                match node.try_finish_checkpoint() {
                    Ok(false) => assert!(std::time::Instant::now() < deadline),
                    Ok(true) => panic!("ambiguous disk failure cannot acknowledge checkpoint"),
                    Err(_) => break,
                }
                std::thread::yield_now();
            }
            assert!(matches!(node.propose(vec![1]), Err(ConsensusError::Failed)));
            drop(node);
            let mut restored = DurableNode::open(config, directory.path()).unwrap();
            let events = restored.drain().unwrap();
            assert_eq!(events.applied_index, index);
            if point == FaultPoint::AfterFenceInstall {
                assert_eq!(events.snapshot.unwrap().data, b"durable-application-prefix");
                assert!(events.committed.is_empty());
            } else {
                assert!(events.snapshot.is_none());
                assert_eq!(events.committed[0].data, b"retained-published-entry");
            }
        }
    }
}
