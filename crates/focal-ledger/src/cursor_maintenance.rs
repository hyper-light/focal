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
        // Expiry releases only projection pins. This maintenance does
        // not request any additional global history retirement.
        let operation = CursorOperation::AdvanceFloor {
            through: self.cursors.checkpoint().floor,
        };
        self.propose_maintenance(now, operation).map(Some)
    }
    /// Renew `consumer`'s lease to `expires_at` by the node's own hand — for
    /// a consumer whose poll found the lease past its half-life (the audit's
    /// F61): one committed entry with no receipt, no request key and no
    /// client bookkeeping, and at most two a term for a consumer that keeps
    /// polling. `None` while the lease is in its first half.
    pub fn propose_cursor_renewal(
        &mut self,
        consumer: ConsumerId,
        generation: u64,
        now: u64,
        expires_at: u64,
    ) -> Result<Option<CursorMaintenance>, LedgerError> {
        self.check()?;
        let row = self
            .cursors
            .get(consumer)
            .ok_or(StreamError::MissingConsumer)?;
        if row.token.generation != generation {
            return Err(StreamError::WrongGeneration.into());
        }
        if !renewal_due(row, now, expires_at) {
            return Ok(None);
        }
        let operation = CursorOperation::Renew {
            consumer,
            generation,
            expires_at,
        };
        self.propose_maintenance(now, operation).map(Some)
    }
    /// One trusted maintenance entry: the leader's, pending until its quorum.
    fn propose_maintenance(
        &mut self,
        now: u64,
        operation: CursorOperation,
    ) -> Result<CursorMaintenance, LedgerError> {
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
                operation,
            },
        };
        let _encode = self.budget.reserve(
            BudgetKind::Control,
            BudgetLane::Completion,
            reference_charge(&envelope)?,
        )?;
        let data = durable_session_v1::encode(CURSOR_MAINTENANCE_MAGIC, &envelope, usize::MAX)?;
        let digest = ContentHash(*blake3::hash(&data).as_bytes());
        let candidate = self.prepare_maintenance(&envelope, digest)?;
        let target = CursorMaintenance {
            revision: candidate.prepared.revision(),
            clock: now,
        };
        self.consensus.propose_in(data, BudgetLane::Completion)?;
        self.pending_maintenance = Some(candidate);
        Ok(target)
    }

    fn prepare_maintenance(
        &self,
        envelope: &MaintenanceEnvelope,
        digest: ContentHash,
    ) -> Result<MaintenanceCandidate, LedgerError> {
        // Floors and positions are values of the stream line (23 §6), whose
        // published end is past the legacy domain sequence on a native
        // ledger; the entry keeps naming the domain sequence it was made at.
        let published = self.stream_published();
        if envelope.schema != 1
            || envelope.ledger != self.ledger
            || envelope.domain_sequence != self.sequence()
            || envelope.replay_floor > published
            || envelope.replay_floor < self.cursors.checkpoint().floor
        {
            return Err(LedgerError::Corrupt);
        }
        // What the node maintains by its own hand, and only that: the clock
        // when a lease is due, a polled lease past its half-life. Every
        // replica judges both from the committed registry and the entry.
        let due = match &envelope.command.operation {
            CursorOperation::AdvanceFloor { through } => {
                *through == self.cursors.checkpoint().floor
                    && self
                        .next_cursor_expiry()
                        .is_some_and(|expires| expires <= envelope.command.now)
            }
            CursorOperation::Renew {
                consumer,
                generation,
                expires_at,
            } => self.cursors.get(*consumer).is_some_and(|row| {
                row.token.generation == *generation
                    && renewal_due(row, envelope.command.now, *expires_at)
            }),
            _ => false,
        };
        if !due {
            return Err(LedgerError::Corrupt);
        }
        let prepared = self.cursors.prepare(&envelope.command, published)?;
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
            // CM1 retains its original tolerant body-suffix rule.
            let (envelope, _): (MaintenanceEnvelope, _) = durable_session_v1::take(
                data.strip_prefix(CURSOR_MAINTENANCE_MAGIC).ok_or(LedgerError::Corrupt)?,
            )?;
            self.prepare_maintenance(&envelope, digest)?
        };
        self.cursors.publish(candidate.prepared)?;
        self.retire_deltas(candidate.replay_floor)?;
        Ok(())
    }
}

/// A lease is renewed once it has passed its half-life: what remains of it
/// is at most half the term the renewal grants. Renewing at the half is the
/// point that keeps the two distances equal — the time between renewals and
/// the time a consumer that keeps polling has to recover a lost renewal —
/// so a consumer polling more often than half a term needs every poll in the
/// remaining half to fail before it expires, and an idle one costs at most
/// two entries a term.
fn renewal_due(row: &focal_stream::CursorRecord, now: u64, expires_at: u64) -> bool {
    let remaining = row.expires_at.saturating_sub(now);
    expires_at
        .checked_sub(now)
        .is_some_and(|term| remaining.checked_mul(2).is_some_and(|twice| twice <= term))
}
