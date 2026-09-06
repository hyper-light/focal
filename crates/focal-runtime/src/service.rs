use crate::{
    pool::{Pool, Work},
    *,
};
use focal_ledger::{Session, State, Submission};
use focal_memory::{Allocation, BudgetKind, BudgetLane, BudgetStats, MemoryBudget};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    ops::Bound::{Excluded, Unbounded},
    sync::Arc,
};

pub const ASSIGNMENT_SCHEMA: &[u8] = b"focal.runtime.assignment.v1";
pub const DIAGNOSTIC_SCHEMA: &[u8] = b"focal.runtime.diagnostic.v1";
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Diagnostic {
    pub schema: u16,
    pub assignment: ArtifactId,
    pub outcome: WorkerOutcome,
}
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DriveReport {
    pub scanned: usize,
    pub dispatched: usize,
    pub committed: usize,
    pub stale: usize,
    pub indeterminate: usize,
    pub active: usize,
    pub pending: Option<RequestKey>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AssignmentStatus {
    pub id: ArtifactId,
    pub run: ValidationRunId,
    pub running: bool,
    pub needs_reconciliation: bool,
}
struct PendingInput {
    input: AuthenticatedInput,
    hash: ContentHash,
    guard: Option<Assignment>,
    _charge: Allocation,
}
struct Active {
    assignment: Assignment,
    cancellation: Cancellation,
    _charge: Allocation,
    running: bool,
    term: u64,
    worker_deadline: u64,
    outcome: Option<WorkerOutcome>,
    completed: bool,
    indeterminate: bool,
}
pub struct Runtime {
    config: RuntimeConfig,
    identity: RuntimeIdentity,
    catalog: Arc<Catalog>,
    clock: Arc<dyn Clock>,
    pool: Pool,
    budget: MemoryBudget,
    _slots: Allocation,
    active: BTreeMap<ArtifactId, Active>,
    clock_value: u64,
    run_cursor: Option<ValidationRunId>,
    claim_cursor: Option<ClaimId>,
    monitor_cursor: Option<MonitorId>,
    pending: Option<PendingInput>,
    local: bool,
}
impl Runtime {
    pub fn new(
        config: RuntimeConfig,
        identity: RuntimeIdentity,
        catalog: Arc<Catalog>,
        reader: Arc<dyn EvidenceReader>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, RuntimeError> {
        let executor = Arc::new(RegistryExecutor::new(Arc::clone(&catalog), reader));
        Self::with_executor(config, identity, catalog, executor, clock)
    }
    pub fn with_executor(
        config: RuntimeConfig,
        identity: RuntimeIdentity,
        catalog: Arc<Catalog>,
        executor: Arc<dyn Executor>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, RuntimeError> {
        if config.workers == 0
            || config.max_inflight < config.workers
            || config.max_evidence_bytes == 0
            || config.max_evidence_artifacts == 0
            || config.max_reason_bytes == 0
            || config.max_scan_items == 0
            || config.max_owner_commands < 4
            || config.max_pending_bytes == 0
            || identity.principal.is_zero()
            || identity.root.is_zero()
            || identity.epoch.0 == 0
        {
            return Err(RuntimeError::Configuration(
                "invalid worker/owner/identity bounds",
            ));
        }
        let budget = MemoryBudget::new(config.memory_bytes, config.completion_reserve_bytes)?;
        let slots = budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                config
                    .max_inflight
                    .checked_mul(4096)
                    .ok_or(RuntimeError::Capacity)?,
            )?
            .commit();
        let pool = Pool::new(&config, executor, Arc::clone(&clock))?;
        Ok(Self {
            config,
            identity,
            catalog,
            clock,
            pool,
            budget,
            _slots: slots,
            active: BTreeMap::new(),
            clock_value: 0,
            run_cursor: None,
            claim_cursor: None,
            monitor_cursor: None,
            pending: None,
            local: false,
        })
    }
    pub fn active_assignments(&self) -> impl Iterator<Item = AssignmentStatus> + '_ {
        self.active.iter().map(|(id, active)| AssignmentStatus {
            id: *id,
            run: active.assignment.run,
            running: active.running,
            needs_reconciliation: active.indeterminate,
        })
    }
    pub fn active_count(&self) -> usize {
        self.active.len()
    }
    pub fn memory_stats(&self) -> BudgetStats {
        self.budget.stats()
    }
    /// Explicit external reconciliation wakeup; no automatic blind effect retry.
    pub fn retry_reconciliation(&mut self, assignment: ArtifactId) -> bool {
        if self
            .active
            .get(&assignment)
            .is_some_and(|a| a.indeterminate && !a.running)
        {
            self.active.remove(&assignment);
            self.run_cursor = None;
            true
        } else {
            false
        }
    }
    /// Frozen input retained across retries/term changes. There is at most one.
    pub fn pending_input(&self) -> Option<&AuthenticatedInput> {
        self.pending.as_ref().map(|pending| &pending.input)
    }
    /// Host-driven quorum adapter. Never polls Session or consumes Raft messages;
    /// the host must pump poll/step and call again after commit or on its tick.
    pub fn drive(&mut self, session: &mut Session) -> Result<DriveReport, RuntimeError> {
        self.drive_mode(session, false)
    }
    /// Embedded wrapper may synchronously poll the sole voter to commit a stage.
    pub fn drive_local(&mut self, session: &mut Session) -> Result<DriveReport, RuntimeError> {
        let status = session.status();
        if status.voters != [status.node_id] || !status.learners.is_empty() {
            return Err(focal_ledger::LedgerError::NotReady {
                leader: status.leader_id,
            }
            .into());
        }
        self.drive_mode(session, true)
    }
    fn drive_mode(
        &mut self,
        session: &mut Session,
        local: bool,
    ) -> Result<DriveReport, RuntimeError> {
        self.local = local;
        let mut report = DriveReport::default();
        let result = self.drive_inner(session, &mut report);
        report.active = self.active.len();
        report.pending = self
            .pending
            .as_ref()
            .map(|pending| request_key(&pending.input));
        match result {
            Ok(()) | Err(RuntimeError::AwaitingCommit(_)) => Ok(report),
            Err(error) => Err(error),
        }
    }
    fn drive_inner(
        &mut self,
        session: &mut Session,
        report: &mut DriveReport,
    ) -> Result<(), RuntimeError> {
        if session.ledger() != self.identity.ledger {
            return Err(RuntimeError::Configuration("wrong session"));
        }
        let status = session.status();
        if !session.is_authoritative() {
            for active in self.active.values_mut() {
                active.cancellation.cancel();
                active.completed = true;
                active.outcome = None;
            }
            // A retired owner may drain finished payloads, but cannot dispatch
            // or publish any recovered result before its next authority barrier.
            self.collect_finished()?;
            self.active.retain(|_, active| active.running);
            return Err(focal_ledger::LedgerError::NotReady {
                leader: status.leader_id,
            }
            .into());
        }
        let now = self.clock.now_ms();
        if now < self.clock_value {
            return Err(RuntimeError::ClockRegression);
        }
        self.clock_value = now;
        self.collect_finished()?;
        let state = session.read_at_least(SessionSeq(0))?;
        if let Some(pending) = &self.pending
            && state
                .receipts
                .get(&request_key(&pending.input))
                .is_some_and(|receipt| receipt.command_hash == pending.hash)
            && let Command::RecordFencedValidationVerdict { verdict, .. } = &pending.input.command
        {
            for active in self.active.values_mut().filter(|active| {
                active.assignment.run == verdict.run && active.assignment.attempt == verdict.attempt
            }) {
                active.completed = true;
                active.outcome = None;
            }
        }
        for active in self.active.values_mut() {
            let valid = active.term == status.term && fence_matches(state, &active.assignment);
            if !valid && !active.completed {
                active.cancellation.cancel();
                active.completed = true;
                active.outcome = None;
                report.stale = report.stale.checked_add(1).ok_or(RuntimeError::Capacity)?;
            }
            if active.running
                && !active.completed
                && !active.indeterminate
                && now >= active.worker_deadline
            {
                active.cancellation.cancel();
                if active
                    .assignment
                    .policy
                    .as_ref()
                    .is_some_and(|policy| policy.retry == RetryContract::Reconcile)
                {
                    active.indeterminate = true;
                } else {
                    active.outcome = Some(WorkerOutcome::error(
                        DiagnosticCode::TimedOut,
                        "validator attempt deadline elapsed",
                    ));
                }
            }
        }
        self.active
            .retain(|_, active| active.running || !active.completed);
        self.advance_pending(session, report)?;
        self.ensure_epoch(session, report)?;
        self.publish_work(session, now, report)
    }
    fn collect_finished(&mut self) -> Result<(), RuntimeError> {
        for _ in 0..self.config.max_inflight {
            let Some(done) = self.pool.receive()? else {
                break;
            };
            if let Some(active) = self.active.get_mut(&done.id) {
                active.running = false;
                if !active.completed && !active.indeterminate {
                    if done.completed_at >= active.worker_deadline
                        || !done.reconciled && done.completed_at >= active.assignment.deadline
                    {
                        if active
                            .assignment
                            .policy
                            .as_ref()
                            .is_some_and(|p| p.retry == RetryContract::Reconcile)
                        {
                            active.indeterminate = true;
                        } else {
                            active.outcome = Some(WorkerOutcome::error(
                                DiagnosticCode::TimedOut,
                                "handler returned after the persisted deadline",
                            ));
                        }
                    } else {
                        match done.outcome {
                            Some(outcome) if outcome.code != DiagnosticCode::Indeterminate => {
                                active.outcome = Some(outcome)
                            }
                            _ => active.indeterminate = true,
                        }
                    }
                }
            }
        }
        Ok(())
    }
    fn publish_work(
        &mut self,
        session: &mut Session,
        now: u64,
        report: &mut DriveReport,
    ) -> Result<(), RuntimeError> {
        let active_ids: Vec<_> = self.active.keys().copied().collect();
        for id in active_ids {
            let mut active = self.active.remove(&id).ok_or(RuntimeError::Corrupt)?;
            let progress = if active.indeterminate
                && !active.completed
                && report
                    .committed
                    .checked_add(2)
                    .is_some_and(|count| count < self.config.max_owner_commands)
            {
                report.indeterminate = report
                    .indeterminate
                    .checked_add(1)
                    .ok_or(RuntimeError::Capacity)?;
                self.persist_diagnostic(
                    session,
                    id,
                    WorkerOutcome::error(
                        DiagnosticCode::Indeterminate,
                        "external effect outcome requires reconciliation",
                    ),
                    true,
                    report,
                )
                .map(|_| ())
            } else if active.outcome.is_some()
                && !active.completed
                && report
                    .committed
                    .checked_add(4)
                    .is_some_and(|count| count <= self.config.max_owner_commands)
            {
                self.finish(
                    session,
                    id,
                    &active.assignment,
                    active
                        .outcome
                        .as_ref()
                        .ok_or(RuntimeError::Corrupt)?
                        .clone(),
                    report,
                )
                .map(|_| {
                    active.completed = true;
                    active.outcome = None;
                })
            } else {
                Ok(())
            };
            if active.running || !active.completed {
                self.active.insert(id, active);
            }
            progress?;
        }
        self.drive_controls(session, now, report)?;
        if report.committed < self.config.max_owner_commands {
            self.discover_runs(session, now, report)?;
        }
        Ok(())
    }
    fn ensure_epoch(
        &mut self,
        session: &mut Session,
        report: &mut DriveReport,
    ) -> Result<(), RuntimeError> {
        let state = session.read_at_least(SessionSeq(0))?;
        if state
            .epochs
            .get(&self.identity.principal)
            .is_some_and(|w| self.identity.epoch < w.minimum)
        {
            return Err(DomainOutcome::refuse(
                ErrorCode::RequestHistoryExpired,
                "runtime request epoch has expired",
            )
            .into());
        }
        if state.epochs.get(&self.identity.principal).is_some_and(|w| {
            w.admitted.contains(&self.identity.epoch) && self.identity.epoch >= w.minimum
        }) {
            return Ok(());
        }
        let epoch = self.identity.epoch;
        self.submit(
            session,
            b"epoch",
            &epoch,
            Command::NegotiateEpoch { epoch },
            0,
            Vec::new(),
            report,
        )?;
        Ok(())
    }
    #[allow(
        clippy::too_many_arguments,
        reason = "Single private boundary keeps request identity, logged time and custody inputs explicit"
    )]
    fn submit<K: Serialize>(
        &mut self,
        session: &mut Session,
        label: &[u8],
        key: &K,
        command: Command,
        logical_time: u64,
        evidence: Vec<EvidenceAttestation>,
        report: &mut DriveReport,
    ) -> Result<MutationReceipt, RuntimeError> {
        let id = stable_id(self.identity.ledger, label, key)?;
        let input = AuthenticatedInput {
            ledger: self.identity.ledger,
            principal: self.identity.principal,
            request_epoch: self.identity.epoch,
            request_id: RequestId(id),
            expected_revision: None,
            authority: AuthorityContext {
                runtime: true,
                cause: Cause::Root(self.identity.root),
                policy_revision: self.identity.policy_revision,
                logical_time,
                evidence,
            },
            command,
        };
        if let Some(pending) = &self.pending {
            return Err(RuntimeError::AwaitingCommit(request_key(&pending.input)));
        }
        let bytes = postcard::experimental::serialized_size(&input)?;
        if bytes > self.config.max_pending_bytes {
            return Err(RuntimeError::Capacity);
        }
        let amount = bytes
            .checked_mul(8)
            .and_then(|amount| amount.checked_add(4096))
            .ok_or(RuntimeError::Capacity)?;
        let charge = self
            .budget
            .reserve(BudgetKind::Pending, BudgetLane::Completion, amount)?
            .commit();
        let guard = self.input_guard(session, &input)?;
        let hash = command_hash(&input)?;
        self.pending = Some(PendingInput {
            input,
            hash,
            guard,
            _charge: charge,
        });
        self.submit_pending(session, report)
    }
    fn submit_pending(
        &mut self,
        session: &mut Session,
        report: &mut DriveReport,
    ) -> Result<MutationReceipt, RuntimeError> {
        let pending = self.pending.as_ref().ok_or(RuntimeError::Corrupt)?;
        let result = if self.local {
            session.submit_local(&pending.input)
        } else {
            session.propose(&pending.input)
        };
        match result? {
            Submission::Committed(receipt) => {
                report.committed = report
                    .committed
                    .checked_add(1)
                    .ok_or(RuntimeError::Capacity)?;
                let pending = self.pending.take().ok_or(RuntimeError::Corrupt)?;
                if let Command::RecordFencedValidationVerdict { verdict, .. } =
                    pending.input.command
                {
                    for active in self.active.values_mut().filter(|active| {
                        active.assignment.run == verdict.run
                            && active.assignment.attempt == verdict.attempt
                    }) {
                        active.completed = true;
                        active.outcome = None;
                    }
                }
                self.run_cursor = None;
                self.claim_cursor = None;
                self.monitor_cursor = None;
                Ok(receipt)
            }
            Submission::Domain(outcome) => {
                self.pending = None;
                Err(outcome.into())
            }
            Submission::Pending(key) => Err(RuntimeError::AwaitingCommit(key)),
        }
    }
    fn advance_pending(
        &mut self,
        session: &mut Session,
        report: &mut DriveReport,
    ) -> Result<(), RuntimeError> {
        let Some(pending) = &self.pending else {
            return Ok(());
        };
        let key = request_key(&pending.input);
        let state = session.read_at_least(SessionSeq(0))?;
        let known = state.receipts.get(&key);
        let replaced_artifact = if known.is_some_and(|receipt| receipt.command_hash != pending.hash)
        {
            // Another owner may win the same logical dispatch/diagnostic ID after
            // our uncommitted proposal was overwritten. Adopt its committed
            // artifact on the next scan; never report our different input committed.
            if let Command::RegisterArtifact { artifact } = &pending.input.command {
                let stored = state
                    .artifacts
                    .get(&artifact.id)
                    .ok_or(RuntimeError::Corrupt)?;
                if pending.guard.is_none()
                    || stored.content().ledger != artifact.content.ledger
                    || stored.content().producer != artifact.content.producer
                    || stored.content().schema_hash != artifact.content.schema_hash
                    || stored.content().kind != artifact.content.kind
                {
                    return Err(RuntimeError::Corrupt);
                }
                true
            } else {
                return Err(RuntimeError::Corrupt);
            }
        } else {
            false
        };
        if replaced_artifact
            || (known.is_none()
                && pending
                    .guard
                    .as_ref()
                    .is_some_and(|guard| !fence_matches(state, guard)))
        {
            self.pending = None;
            report.stale = report.stale.checked_add(1).ok_or(RuntimeError::Capacity)?;
            self.run_cursor = None;
            return Ok(());
        }
        self.submit_pending(session, report).map(|_| ())
    }
    fn input_guard(
        &self,
        session: &Session,
        input: &AuthenticatedInput,
    ) -> Result<Option<Assignment>, RuntimeError> {
        match &input.command {
            Command::RegisterArtifact { artifact }
                if artifact.content.schema_hash
                    == ContentHash(*blake3::hash(ASSIGNMENT_SCHEMA).as_bytes()) =>
            {
                let ArtifactPayload::Inline(bytes) = &artifact.content.payload else {
                    return Err(RuntimeError::Corrupt);
                };
                Ok(Some(postcard::from_bytes(bytes)?))
            }
            Command::RegisterArtifact { artifact }
                if artifact.content.schema_hash
                    == ContentHash(*blake3::hash(DIAGNOSTIC_SCHEMA).as_bytes()) =>
            {
                let ArtifactPayload::Inline(bytes) = &artifact.content.payload else {
                    return Err(RuntimeError::Corrupt);
                };
                let diagnostic: Diagnostic = postcard::from_bytes(bytes)?;
                Ok(Some(decode_artifact(
                    session.read_at_least(SessionSeq(0))?,
                    diagnostic.assignment,
                    ASSIGNMENT_SCHEMA,
                    self.identity.principal,
                )?))
            }
            Command::RecordFencedValidationVerdict { verdict, receipt } => {
                let id = ArtifactId(stable_id(
                    self.identity.ledger,
                    b"assignment",
                    &(verdict.run, verdict.attempt, &verdict.handler, receipt),
                )?);
                Ok(Some(decode_artifact(
                    session.read_at_least(SessionSeq(0))?,
                    id,
                    ASSIGNMENT_SCHEMA,
                    self.identity.principal,
                )?))
            }
            _ => Ok(None),
        }
    }
    fn register_artifact(
        &mut self,
        session: &mut Session,
        id: ArtifactId,
        kind: &str,
        schema: &[u8],
        bytes: Vec<u8>,
        report: &mut DriveReport,
    ) -> Result<ArtifactRef, RuntimeError> {
        if let Some(existing) = session.read_at_least(SessionSeq(0))?.artifacts.get(&id) {
            return Ok(ArtifactRef {
                id,
                hash: existing.content_hash(),
            });
        }
        let artifact = NewArtifact {
            id,
            content: ArtifactContent {
                ledger: self.identity.ledger,
                schema: 1,
                kind: kind.into(),
                schema_hash: ContentHash(*blake3::hash(schema).as_bytes()),
                metadata: Vec::new(),
                payload: ArtifactPayload::Inline(bytes),
                producer: self.identity.principal,
                receipt: None,
                inputs: Default::default(),
                visibility: Default::default(),
            },
        };
        let descriptor_hash = artifact.content.content_hash()?;
        // Inline bytes and their fixed schema travel inside this same fsynced
        // mutation. No external-content durability claim is synthesized here.
        self.submit(
            session,
            b"artifact",
            &id,
            Command::RegisterArtifact { artifact },
            0,
            vec![EvidenceAttestation {
                descriptor_hash,
                custody_revision: 1,
                durable: true,
                schema_valid: true,
            }],
            report,
        )?;
        let stored = session
            .read_at_least(SessionSeq(0))?
            .artifacts
            .get(&id)
            .ok_or(RuntimeError::Corrupt)?;
        Ok(ArtifactRef {
            id,
            hash: stored.content_hash(),
        })
    }
    fn persist_diagnostic(
        &mut self,
        session: &mut Session,
        assignment: ArtifactId,
        outcome: WorkerOutcome,
        indeterminate: bool,
        report: &mut DriveReport,
    ) -> Result<(ArtifactRef, Diagnostic), RuntimeError> {
        let id = ArtifactId(stable_id(
            self.identity.ledger,
            if indeterminate {
                b"indeterminate"
            } else {
                b"diagnostic"
            },
            &assignment,
        )?);
        let proposed = Diagnostic {
            schema: 1,
            assignment,
            outcome,
        };
        let reference = self.register_artifact(
            session,
            id,
            if matches!(
                proposed.outcome.value,
                VerdictValue::Incomplete | VerdictValue::Error
            ) {
                "error"
            } else {
                "validation-result"
            },
            DIAGNOSTIC_SCHEMA,
            postcard::to_stdvec(&proposed)?,
            report,
        )?;
        let stored: Diagnostic = decode_artifact(
            session.read_at_least(SessionSeq(0))?,
            id,
            DIAGNOSTIC_SCHEMA,
            self.identity.principal,
        )?;
        if stored.schema != 1
            || stored.assignment != assignment
            || stored.outcome.reason.len() > self.config.max_reason_bytes
        {
            return Err(RuntimeError::Corrupt);
        }
        Ok((reference, stored))
    }
    fn finish(
        &mut self,
        session: &mut Session,
        id: ArtifactId,
        assignment: &Assignment,
        outcome: WorkerOutcome,
        report: &mut DriveReport,
    ) -> Result<(), RuntimeError> {
        let (diagnostic, stored) = self.persist_diagnostic(session, id, outcome, false, report)?;
        let mut evidence = assignment.evidence.clone();
        evidence.push(diagnostic);
        evidence.sort();
        evidence.dedup();
        let verdict = VerdictRecord {
            run: assignment.run,
            evaluator: self.identity.principal,
            handler: assignment.handler.clone(),
            attempt: assignment.attempt,
            manifest: assignment.manifest,
            value: stored.outcome.value,
            evidence,
        };
        self.submit(
            session,
            b"verdict",
            &id,
            Command::RecordFencedValidationVerdict {
                verdict,
                receipt: assignment.receipt,
            },
            assignment.started_at,
            Vec::new(),
            report,
        )?;
        Ok(())
    }
    fn discover_runs(
        &mut self,
        session: &mut Session,
        now: u64,
        report: &mut DriveReport,
    ) -> Result<(), RuntimeError> {
        let ids: Vec<_> = session
            .read_at_least(SessionSeq(0))?
            .runs
            .range((self.run_cursor.map_or(Unbounded, Excluded), Unbounded))
            .take(self.config.max_scan_items)
            .map(|(id, _)| *id)
            .collect();
        if ids.is_empty() {
            self.run_cursor = None;
            return Ok(());
        }
        for run_id in ids {
            self.run_cursor = Some(run_id);
            report.scanned = report
                .scanned
                .checked_add(1)
                .ok_or(RuntimeError::Capacity)?;
            if self.active.len() >= self.config.max_inflight
                || report.committed >= self.config.max_owner_commands
            {
                break;
            }
            let state = session.read_at_least(SessionSeq(0))?;
            let run = state.runs.get(&run_id).ok_or(RuntimeError::Corrupt)?;
            if run.final_verdict.is_some() || run.evaluator != self.identity.principal {
                continue;
            }
            let validation = state
                .validations
                .get(&run_id.validation)
                .ok_or(RuntimeError::Corrupt)?;
            let claim = state.claims.get(&run.claim).ok_or(RuntimeError::Corrupt)?;
            let handler = validation
                .content()
                .handlers
                .get(run.handler_index as usize)
                .ok_or(RuntimeError::Corrupt)?
                .clone();
            let attempt = u32::try_from(run.attempts.len()).map_err(|_| RuntimeError::Capacity)?;
            let receipt = claim.lifecycle().receipt.as_ref().map(|r| r.fence);
            let id = ArtifactId(stable_id(
                self.identity.ledger,
                b"assignment",
                &(run_id, attempt, &handler, receipt),
            )?);
            if self.active.contains_key(&id) {
                continue;
            }
            let required_policy = validation.content().policy_revision;
            let registration = self.catalog.registration(&handler);
            let policy = registration.map(|(_, p)| p.clone());
            if policy.as_ref().is_some_and(|p| {
                self.active
                    .values()
                    .filter(|a| (a.running || !a.completed) && a.assignment.handler == handler)
                    .count()
                    >= p.max_concurrency
            }) {
                continue;
            }
            let evidence = exact_manifest(state, run, self.config.max_scan_items)?;
            if evidence.len() > self.config.max_evidence_artifacts {
                return Err(RuntimeError::Capacity);
            }
            let assignment = Assignment {
                schema: 1,
                ledger: self.identity.ledger,
                run: run_id,
                claim: run.claim,
                receipt,
                evaluator: run.evaluator,
                handler,
                attempt,
                manifest: run.manifest,
                evidence: evidence.to_vec(),
                quality_bar: run
                    .quality_phase
                    .then(|| validation.content().quality_bar.clone())
                    .flatten(),
                policy: policy.clone(),
                started_at: now,
                deadline: now
                    .checked_add(policy.as_ref().map_or(1, |p| p.timeout_ms))
                    .ok_or(RuntimeError::Capacity)?,
            };
            if !fence_matches(state, &assignment) {
                continue;
            }
            let recovered = state.artifacts.contains_key(&id);
            let assignment = if recovered {
                decode_artifact::<Assignment>(
                    state,
                    id,
                    ASSIGNMENT_SCHEMA,
                    self.identity.principal,
                )?
            } else {
                assignment
            };
            if assignment.schema != 1
                || assignment.ledger != self.identity.ledger
                || !fence_matches(state, &assignment)
            {
                return Err(RuntimeError::Corrupt);
            }
            let mut bytes = 0usize;
            for reference in &assignment.evidence {
                let artifact = state
                    .artifacts
                    .get(&reference.id)
                    .ok_or(RuntimeError::Corrupt)?;
                if artifact.content_hash() != reference.hash {
                    return Err(RuntimeError::Corrupt);
                }
                let size = match &artifact.content().payload {
                    ArtifactPayload::Inline(b) => b.len(),
                    ArtifactPayload::Content(reference) => {
                        usize::try_from(reference.length).map_err(|_| RuntimeError::Capacity)?
                    }
                };
                bytes = bytes.checked_add(size).ok_or(RuntimeError::Capacity)?;
                if bytes > self.config.max_evidence_bytes {
                    return Err(RuntimeError::Capacity);
                }
            }
            let descriptor_bytes = postcard::experimental::serialized_size(&assignment)?
                .checked_mul(4)
                .ok_or(RuntimeError::Capacity)?;
            let charge_bytes = bytes
                .checked_mul(3)
                .and_then(|n| n.checked_add(16384))
                .and_then(|n| n.checked_add(descriptor_bytes))
                .and_then(|n| {
                    self.config
                        .max_reason_bytes
                        .checked_mul(2)
                        .and_then(|reason| n.checked_add(reason))
                })
                .ok_or(RuntimeError::Capacity)?;
            let charge = self
                .budget
                .reserve(BudgetKind::Pending, BudgetLane::Ordinary, charge_bytes)?
                .commit();
            // The owner retains only frozen assignment metadata and any verdict.
            // Worker payload ownership moves through Work -> Finished, preserving
            // its charge even if the owner is dropped while a callback still runs.
            let owner_bytes = descriptor_bytes
                .checked_add(16384)
                .and_then(|n| {
                    self.config
                        .max_reason_bytes
                        .checked_mul(2)
                        .and_then(|reason| n.checked_add(reason))
                })
                .ok_or(RuntimeError::Capacity)?;
            let owner_charge = self
                .budget
                .reserve(BudgetKind::Pending, BudgetLane::Ordinary, owner_bytes)?
                .commit();
            let artifacts = assignment
                .evidence
                .iter()
                .map(|reference| {
                    let artifact = state
                        .artifacts
                        .get(&reference.id)
                        .ok_or(RuntimeError::Corrupt)?;
                    Ok(EvidenceArtifact {
                        reference: *reference,
                        schema: artifact.content().schema_hash,
                        payload: artifact.content().payload.clone(),
                    })
                })
                .collect::<Result<Vec<_>, RuntimeError>>()?;
            if !recovered {
                self.register_artifact(
                    session,
                    id,
                    "runtime-dispatch",
                    ASSIGNMENT_SCHEMA,
                    postcard::to_stdvec(&assignment)?,
                    report,
                )?;
            }
            let task = Task {
                id,
                assignment: assignment.clone(),
                evidence: artifacts,
                fresh_assignment: !recovered,
                max_evidence_bytes: self.config.max_evidence_bytes,
            };
            let cancellation = Cancellation::new();
            let diagnostic_id = ArtifactId(stable_id(self.identity.ledger, b"diagnostic", &id)?);
            let outcome = if session
                .read_at_least(SessionSeq(0))?
                .artifacts
                .contains_key(&diagnostic_id)
            {
                Some(
                    decode_artifact::<Diagnostic>(
                        session.read_at_least(SessionSeq(0))?,
                        diagnostic_id,
                        DIAGNOSTIC_SCHEMA,
                        self.identity.principal,
                    )?
                    .outcome,
                )
            } else if policy.as_ref().is_some_and(|p| {
                p.revision != required_policy || p.revision > self.identity.policy_revision
            }) {
                Some(WorkerOutcome::error(
                    DiagnosticCode::PolicyChanged,
                    "validator policy revision is not authorized for the immutable requirement",
                ))
            } else if task.assignment.policy != policy {
                Some(WorkerOutcome::error(
                    DiagnosticCode::PolicyChanged,
                    "persisted dispatch policy differs from registered policy",
                ))
            } else if policy.is_none() {
                Some(WorkerOutcome::error(
                    DiagnosticCode::HandlerUnavailable,
                    "pinned handler has no registered execution policy",
                ))
            } else if now >= task.assignment.deadline
                && policy
                    .as_ref()
                    .is_some_and(|p| p.retry == RetryContract::ReadOnly)
            {
                Some(WorkerOutcome::error(
                    DiagnosticCode::TimedOut,
                    "persisted attempt deadline elapsed",
                ))
            } else {
                None
            };
            let running = outcome.is_none();
            if running {
                self.pool.submit(Work {
                    task,
                    cancellation: cancellation.clone(),
                    charge,
                })?;
                report.dispatched = report
                    .dispatched
                    .checked_add(1)
                    .ok_or(RuntimeError::Capacity)?;
            }
            let worker_deadline = match &policy {
                Some(policy) if policy.retry == RetryContract::Reconcile => now
                    .checked_add(policy.timeout_ms)
                    .ok_or(RuntimeError::Capacity)?,
                _ => assignment.deadline,
            };
            self.active.insert(
                id,
                Active {
                    assignment,
                    cancellation,
                    _charge: owner_charge,
                    running,
                    term: session.status().term,
                    worker_deadline,
                    outcome,
                    completed: false,
                    indeterminate: false,
                },
            );
        }
        Ok(())
    }
    fn drive_controls(
        &mut self,
        session: &mut Session,
        now: u64,
        report: &mut DriveReport,
    ) -> Result<(), RuntimeError> {
        if report.committed >= self.config.max_owner_commands {
            return Ok(());
        }
        let remaining = self
            .config
            .max_owner_commands
            .checked_sub(report.committed)
            .ok_or(RuntimeError::Corrupt)?;
        let claim_command_limit = report
            .committed
            .checked_add((remaining / 2).max(1))
            .ok_or(RuntimeError::Capacity)?;
        let claims: Vec<_> = session
            .read_at_least(SessionSeq(0))?
            .claims
            .range((self.claim_cursor.map_or(Unbounded, Excluded), Unbounded))
            .take(self.config.max_scan_items)
            .map(|(id, _)| *id)
            .collect();
        if claims.is_empty() {
            self.claim_cursor = None;
        }
        for id in claims {
            if report.committed >= claim_command_limit {
                break;
            }
            self.claim_cursor = Some(id);
            report.scanned = report
                .scanned
                .checked_add(1)
                .ok_or(RuntimeError::Capacity)?;
            let state = session.read_at_least(SessionSeq(0))?;
            let claim = state.claims.get(&id).ok_or(RuntimeError::Corrupt)?;
            let command = if !claim.lifecycle().status.is_terminal()
                && claim.content().deadline.is_some_and(|d| d.at <= now)
            {
                let d = claim.content().deadline.ok_or(RuntimeError::Corrupt)?;
                Some((
                    Command::ExpireClaim {
                        claim: id,
                        timer: d.timer,
                        generation: d.generation,
                        fired_at: d.at,
                    },
                    d.at,
                ))
            } else {
                match claim.lifecycle().status {
                    ClaimStatus::TestamentGenerated => Some((
                        Command::AcknowledgeTestament {
                            claim: id,
                            testament: claim.lifecycle().testament.ok_or(RuntimeError::Corrupt)?,
                        },
                        0,
                    )),
                    ClaimStatus::TestamentAcknowledged
                        if phase_finished(state, id, ValidationPhase::Increment) =>
                    {
                        Some((Command::BeginWholeWorkValidation { claim: id }, 0))
                    }
                    ClaimStatus::Validating
                        if phase_finished(state, id, ValidationPhase::WholeWork) =>
                    {
                        Some((Command::CompleteWholeWork { claim: id }, 0))
                    }
                    _ => None,
                }
            };
            if let Some((command, logical_time)) = command {
                let key = command.clone();
                self.submit(
                    session,
                    b"control",
                    &key,
                    command,
                    logical_time,
                    Vec::new(),
                    report,
                )?;
            }
        }
        let monitors: Vec<_> = session
            .read_at_least(SessionSeq(0))?
            .monitors
            .range((self.monitor_cursor.map_or(Unbounded, Excluded), Unbounded))
            .take(self.config.max_scan_items)
            .map(|(id, _)| *id)
            .collect();
        if monitors.is_empty() {
            self.monitor_cursor = None;
        }
        for id in monitors {
            if report.committed >= self.config.max_owner_commands {
                break;
            }
            self.monitor_cursor = Some(id);
            report.scanned = report
                .scanned
                .checked_add(1)
                .ok_or(RuntimeError::Capacity)?;
            let state = session.read_at_least(SessionSeq(0))?;
            let monitor = state.monitors.get(&id).ok_or(RuntimeError::Corrupt)?;
            if monitor.released.is_some()
                || monitor.deadline.at > now
                || state
                    .claims
                    .get(&monitor.owner)
                    .is_none_or(|c| c.lifecycle().status.is_terminal())
            {
                continue;
            }
            let d = monitor.deadline;
            let command = Command::ExpireMonitor {
                monitor: id,
                timer: d.timer,
                generation: d.generation,
                fired_at: d.at,
            };
            let key = command.clone();
            self.submit(session, b"control", &key, command, d.at, Vec::new(), report)?;
        }
        Ok(())
    }
}
impl Drop for Runtime {
    fn drop(&mut self) {
        for active in self.active.values() {
            active.cancellation.cancel();
        }
    }
}
fn request_key(input: &AuthenticatedInput) -> RequestKey {
    RequestKey {
        principal: input.principal,
        epoch: input.request_epoch,
        id: input.request_id,
    }
}
fn stable_id<T: Serialize>(
    ledger: LedgerId,
    label: &[u8],
    value: &T,
) -> Result<[u8; 16], RuntimeError> {
    let mut hash = blake3::Hasher::new_derive_key("focal.runtime.identity.v1");
    hash.update(label);
    hash.update(&postcard::to_stdvec(&(ledger, value))?);
    let mut id = [0; 16];
    for (output, input) in id.iter_mut().zip(hash.finalize().as_bytes()) {
        *output = *input;
    }
    Ok(id)
}
fn decode_artifact<T: for<'de> Deserialize<'de>>(
    state: &State,
    id: ArtifactId,
    schema: &[u8],
    principal: ParticipantId,
) -> Result<T, RuntimeError> {
    let artifact = state.artifacts.get(&id).ok_or(RuntimeError::Corrupt)?;
    if artifact.content().schema_hash != ContentHash(*blake3::hash(schema).as_bytes())
        || artifact.content().producer != principal
    {
        return Err(RuntimeError::Corrupt);
    }
    let ArtifactPayload::Inline(bytes) = &artifact.content().payload else {
        return Err(RuntimeError::Corrupt);
    };
    Ok(postcard::from_bytes(bytes)?)
}
fn exact_manifest<'a>(
    state: &'a State,
    run: &ValidationRun,
    max_visits: usize,
) -> Result<&'a [ArtifactRef], RuntimeError> {
    if run.manifest == manifest_hash(&[])? {
        return Ok(&[]);
    }
    if run.id.phase == ValidationPhase::WholeWork
        && let Some(object) = state
            .identities
            .get(&(ObjectKind::Testament, run.id.target_hash))
        && let Some(testament) = state.testaments.get(&TestamentId(object.0))
        && testament.content().claim == run.claim
        && manifest_hash(&testament.content().artifacts)? == run.manifest
    {
        return Ok(&testament.content().artifacts);
    }
    if let Some(claim) = state.claims.get(&run.claim)
        && let Some(set) = claim
            .lifecycle()
            .evidence_set
            .and_then(|id| state.evidence_sets.get(&id))
        && manifest_hash(&set.artifacts)? == run.manifest
    {
        return Ok(&set.artifacts);
    }
    // Historical increment manifests need the future durable manifest index.
    // This bounded fallback never substitutes a newer mutable evidence set.
    let candidates = state
        .testaments
        .values()
        .map(|t| (t.content().claim, t.content().artifacts.as_slice()))
        .chain(
            state
                .evidence_sets
                .values()
                .map(|set| (set.claim, set.artifacts.as_slice())),
        );
    for (claim, references) in candidates.take(max_visits) {
        if claim == run.claim && manifest_hash(references)? == run.manifest {
            return Ok(references);
        }
    }
    Err(RuntimeError::Configuration(
        "exact historical increment manifest is unavailable within the read budget",
    ))
}
fn phase_finished(state: &State, claim: ClaimId, phase: ValidationPhase) -> bool {
    state
        .validations
        .iter()
        .filter(|(_, v)| {
            v.content().claim == claim
                && v.content().phase == phase
                && v.content().mode == ValidationMode::Required
        })
        .all(|(id, _)| {
            let mut runs = state
                .runs
                .values()
                .filter(|r| r.id.validation == *id)
                .peekable();
            runs.peek().is_some() && runs.all(|r| r.final_verdict.is_some())
        })
}
fn fence_matches(state: &State, assignment: &Assignment) -> bool {
    let Some(run) = state.runs.get(&assignment.run) else {
        return false;
    };
    let Some(claim) = state.claims.get(&run.claim) else {
        return false;
    };
    let Some(validation) = state.validations.get(&assignment.run.validation) else {
        return false;
    };
    if run.final_verdict.is_some()
        || run.attempts.len() != assignment.attempt as usize
        || run.manifest != assignment.manifest
        || run.evaluator != assignment.evaluator
        || run.claim != assignment.claim
        || assignment.quality_bar
            != run
                .quality_phase
                .then(|| validation.content().quality_bar.clone())
                .flatten()
        || validation
            .content()
            .handlers
            .get(run.handler_index as usize)
            != Some(&assignment.handler)
        || claim.lifecycle().receipt.as_ref().map(|r| r.fence) != assignment.receipt
    {
        return false;
    }
    let status = claim.lifecycle().status;
    if matches!(
        status,
        ClaimStatus::Cancelled
            | ClaimStatus::Revoked
            | ClaimStatus::Superseded
            | ClaimStatus::Expired
            | ClaimStatus::Deadlocked
            | ClaimStatus::DependencyFailed
    ) {
        return false;
    }
    if status.is_terminal() {
        return validation.content().mode == ValidationMode::Observe;
    }
    match assignment.run.phase {
        ValidationPhase::Admission => status == ClaimStatus::Posted,
        ValidationPhase::Increment => matches!(
            status,
            ClaimStatus::Received
                | ClaimStatus::Progressed
                | ClaimStatus::TestamentGenerated
                | ClaimStatus::TestamentAcknowledged
                | ClaimStatus::Validating
        ),
        ValidationPhase::WholeWork => status == ClaimStatus::Validating,
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
