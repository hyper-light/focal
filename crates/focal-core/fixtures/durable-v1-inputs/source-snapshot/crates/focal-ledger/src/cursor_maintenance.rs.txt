/// A pending maintenance condition, not a committed receipt. A fleet host must
/// continue its quorum pump until both committed counters meet this condition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CursorMaintenance {
    pub revision: u64,
    pub clock: u64,
}

#[derive(Serialize, Deserialize)]
struct MaintenanceEnvelope {
    schema: u16,
    ledger: LedgerId,
    domain_sequence: SessionSeq,
    replay_floor: SessionSeq,
    command: CursorCommand,
}
struct MaintenanceCandidate {
    digest: ContentHash,
    prepared: PreparedCursorUpdate,
    replay_floor: SessionSeq,
}

impl Session {
    /// Next live/seed projection lease that has not yet expired in the committed
    /// logical clock. Protected consumers and already released pins are ignored.
    pub fn next_cursor_expiry(&self) -> Option<u64> {
        self.cursors
            .checkpoint()
            .consumers
            .values()
            .filter(|record| {
                matches!(
                    record.mode,
                    focal_stream::CursorMode::Live | focal_stream::CursorMode::Seeding { .. }
                )
            })
            .map(|record| record.expires_at)
            .filter(|expires| *expires > self.cursor_clock())
            .min()
    }

    /// Trusted single-voter runtime maintenance. A due projection lease advances
    /// the cursor clock through the same durable log, without a client request
    /// epoch or an ever-growing receipt table. Idle ticks do not write anything.
    /// The caller supplies wall time explicitly; replay never reads ambient time.
    pub fn maintain_cursor_clock_local(&mut self, now: u64) -> Result<bool, LedgerError> {
        self.check()?;
        if self
            .next_cursor_expiry()
            .is_none_or(|expires| expires > now)
        {
            return Ok(false);
        }
        let status = self.status();
        if status.voters != [status.node_id] || !status.learners.is_empty() {
            return Err(LedgerError::NotReady {
                leader: status.leader_id,
            });
        }
        let Some(target) = self.propose_cursor_clock(now)? else {
            return Ok(false);
        };
        let events = self.poll()?;
        if !events.messages.is_empty()
            || self.cursor_clock() < target.clock
            || self.cursor_revision() < target.revision
        {
            return Err(LedgerError::OutcomeUnknown);
        }
        Ok(true)
    }

    /// Propose trusted clock maintenance for a quorum-driven host. `Some` means
    /// pending only; `None` means no projection lease is due. No receipt or
    /// request epoch is allocated. Retrying after lost leadership is safe because
    /// already committed expiry removes that due condition.
    pub fn propose_cursor_clock(
        &mut self,
        now: u64,
    ) -> Result<Option<CursorMaintenance>, LedgerError> {
        self.check()?;
        if self
            .next_cursor_expiry()
            .is_none_or(|expires| expires > now)
        {
            return Ok(None);
        }
        let status = self.status();
        if status.role != StateRole::Leader || self.ready_term != Some(status.term) {
            return Err(LedgerError::NotReady {
                leader: status.leader_id,
            });
        }
        if self.pending_count() > 0 {
            return Err(LedgerError::Capacity);
        }
        let envelope = MaintenanceEnvelope {
            schema: 1,
            ledger: self.ledger,
            domain_sequence: self.sequence(),
            replay_floor: self.stream_bounds().floor,
            command: CursorCommand {
                expected_revision: self.cursor_revision(),
                now,
                // Expiry releases only projection pins. This maintenance does
                // not request any additional global history retirement.
                operation: CursorOperation::AdvanceFloor {
                    through: self.cursors.checkpoint().floor,
                },
            },
        };
        let _encode = self.budget.reserve(
            BudgetKind::Control,
            BudgetLane::Completion,
            reference_charge(&envelope)?,
        )?;
        let mut data = CURSOR_MAINTENANCE_MAGIC.to_vec();
        data.extend(postcard::to_stdvec(&envelope)?);
        let digest = ContentHash(*blake3::hash(&data).as_bytes());
        let candidate = self.prepare_maintenance(&envelope, digest)?;
        let target = CursorMaintenance {
            revision: candidate.prepared.checkpoint().revision,
            clock: now,
        };
        self.consensus.propose_in(data, BudgetLane::Completion)?;
        self.pending_maintenance = Some(candidate);
        Ok(Some(target))
    }

    fn prepare_maintenance(
        &self,
        envelope: &MaintenanceEnvelope,
        digest: ContentHash,
    ) -> Result<MaintenanceCandidate, LedgerError> {
        if envelope.schema != 1
            || envelope.ledger != self.ledger
            || envelope.domain_sequence != self.sequence()
            || envelope.replay_floor > self.sequence()
            || envelope.replay_floor < self.cursors.checkpoint().floor
            || envelope.command.operation
                != (CursorOperation::AdvanceFloor {
                    through: self.cursors.checkpoint().floor,
                })
            || self
                .next_cursor_expiry()
                .is_none_or(|expires| expires > envelope.command.now)
        {
            return Err(LedgerError::Corrupt);
        }
        let prepared = self.cursors.prepare(&envelope.command, self.sequence())?;
        Ok(MaintenanceCandidate {
            digest,
            prepared,
            replay_floor: envelope.replay_floor,
        })
    }

    fn apply_maintenance_entry(&mut self, data: &[u8]) -> Result<(), LedgerError> {
        self.clear_pending();
        self.pending_cursor = None;
        let digest = ContentHash(*blake3::hash(data).as_bytes());
        let candidate = if self
            .pending_maintenance
            .as_ref()
            .is_some_and(|candidate| candidate.digest == digest)
        {
            self.pending_maintenance
                .take()
                .ok_or(LedgerError::Corrupt)?
        } else {
            self.pending_maintenance = None;
            let _decode = self.budget.reserve(
                BudgetKind::Recovery,
                BudgetLane::Completion,
                data.len()
                    .checked_mul(64)
                    .and_then(|n| n.checked_add(4096))
                    .ok_or(LedgerError::Capacity)?,
            )?;
            let envelope: MaintenanceEnvelope =
                postcard::from_bytes(data.strip_prefix(CURSOR_MAINTENANCE_MAGIC).ok_or(LedgerError::Corrupt)?)?;
            self.prepare_maintenance(&envelope, digest)?
        };
        self.cursors.publish(candidate.prepared)?;
        self.retire_deltas(candidate.replay_floor)?;
        Ok(())
    }
}
