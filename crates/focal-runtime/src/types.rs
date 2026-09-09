use focal_evidence::{Evaluation, Registration, Registry, RegistryError, Validator};
use focal_memory::MemoryError;
use focal_model::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    /// Internal staged-control suspension, converted to DriveReport::pending.
    #[error("runtime command awaits durable commitment: {0:?}")]
    AwaitingCommit(RequestKey),
    #[error("canonical identity: {0}")]
    Canonical(#[from] CanonicalError),
    #[error("invalid runtime configuration: {0}")]
    Configuration(&'static str),
    #[error("runtime bounded capacity is exhausted")]
    Capacity,
    #[error("runtime clock moved backward")]
    ClockRegression,
    #[error("ledger: {0}")]
    Ledger(#[from] focal_ledger::LedgerError),
    #[error("domain: {0}")]
    Domain(#[from] DomainOutcome),
    #[error("registry: {0}")]
    Registry(#[from] RegistryError),
    #[error("memory: {0}")]
    Memory(#[from] MemoryError),
    #[error("codec: {0}")]
    Codec(#[from] postcard::Error),
    #[error("persisted runtime assignment or outcome is corrupt")]
    Corrupt,
    #[error("worker service is stopped")]
    Stopped,
}

impl RuntimeError {
    /// The host may continue consensus progress and retry on its next bounded
    /// wakeup. This does not promise that configured capacity will become free.
    /// Corruption, durability failure and invalid configuration remain fatal.
    pub fn is_retryable(&self) -> bool {
        use focal_consensus::ConsensusError;
        use focal_graph::GraphError;
        use focal_ledger::LedgerError;
        match self {
            Self::AwaitingCommit(_)
            | Self::Capacity
            | Self::Canonical(CanonicalError::Capacity) => true,
            Self::Memory(error)
            | Self::Ledger(LedgerError::Memory(error))
            | Self::Ledger(LedgerError::Graph(GraphError::Memory(error))) => {
                matches!(
                    error,
                    MemoryError::Capacity { .. }
                        | MemoryError::DiskCapacity { .. }
                        | MemoryError::AllocationFailed
                )
            }
            Self::Ledger(
                LedgerError::Capacity
                | LedgerError::NotReady { .. }
                | LedgerError::OutcomeUnknown
                | LedgerError::Behind
                | LedgerError::Consensus(ConsensusError::Capacity | ConsensusError::NotLeader { .. }),
            ) => true,
            Self::Domain(DomainOutcome::Refuse { code, .. }) => matches!(
                code,
                ErrorCode::Capacity
                    | ErrorCode::StaleReceipt
                    | ErrorCode::StaleEvaluator
                    | ErrorCode::RevisionConflict
                    | ErrorCode::DeadlineNotDue
                    | ErrorCode::InvalidTransition
            ),
            Self::Domain(DomainOutcome::Inform { reason, .. }) => {
                !matches!(reason, InformReason::StandingDenied)
            }
            _ => false,
        }
    }
}

/// Implementations must not panic or block: the owner and workers use this clock
/// for admission and completion timestamps. All values are milliseconds.
pub trait Clock: Send + Sync {
    fn now_ms(&self) -> u64;
}
pub struct SystemClock;
impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
    }
}
#[derive(Debug, Clone)]
pub struct RuntimeIdentity {
    pub ledger: LedgerId,
    pub principal: ParticipantId,
    pub epoch: RequestEpoch,
    pub root: RootCommandId,
    pub policy_revision: u64,
}
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub workers: usize,
    pub max_inflight: usize,
    pub max_evidence_bytes: usize,
    pub max_evidence_artifacts: usize,
    pub max_reason_bytes: usize,
    pub max_scan_items: usize,
    pub max_owner_commands: usize,
    /// Maximum encoded bytes in the single retained protocol input.
    pub max_pending_bytes: usize,
    pub memory_bytes: usize,
    pub completion_reserve_bytes: usize,
}
impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            workers: 2,
            max_inflight: 8,
            max_evidence_bytes: 1024 * 1024,
            max_evidence_artifacts: 64,
            max_reason_bytes: 1024,
            max_scan_items: 256,
            max_owner_commands: 32,
            max_pending_bytes: 128 * 1024,
            memory_bytes: 32 * 1024 * 1024,
            completion_reserve_bytes: 4 * 1024 * 1024,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetryContract {
    /// Deterministic or read-only evaluation can safely repeat after a crash.
    ReadOnly,
    /// Consult the external system by logical run/attempt before every execution,
    /// including the first local dispatch and receipt adoption. Unknown outcome
    /// parks the obligation; it never permits blind execution.
    Reconcile,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionPolicy {
    pub revision: u64,
    pub timeout_ms: u64,
    pub max_concurrency: usize,
    pub retry: RetryContract,
}
pub struct Catalog {
    pub(crate) registry: Registry,
    entries: BTreeMap<(ValidatorId, ContentHash), (Registration, ExecutionPolicy)>,
}
impl Catalog {
    pub fn new(capacity: usize) -> Self {
        Self {
            registry: Registry::new(capacity),
            entries: BTreeMap::new(),
        }
    }
    pub fn register(
        &mut self,
        registration: Registration,
        policy: ExecutionPolicy,
        implementation: Box<dyn Validator>,
    ) -> Result<(), RuntimeError> {
        if policy.revision == 0
            || policy.timeout_ms == 0
            || policy.max_concurrency == 0
            || registration.max_evidence_bytes == 0
            || registration.handler.id.is_zero()
        {
            return Err(RuntimeError::Configuration("zero validator policy bound"));
        }
        let key = (registration.handler.id, registration.handler.version);
        if self
            .entries
            .get(&key)
            .is_some_and(|old| old != &(registration.clone(), policy.clone()))
        {
            return Err(RegistryError::Conflict.into());
        }
        self.registry
            .register(registration.clone(), implementation)?;
        self.entries.insert(key, (registration, policy));
        Ok(())
    }
    pub fn registration(&self, handler: &HandlerRef) -> Option<(&Registration, &ExecutionPolicy)> {
        self.entries
            .get(&(handler.id, handler.version))
            .filter(|(registration, _)| registration.handler == *handler)
            .map(|(r, p)| (r, p))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiagnosticCode {
    Evaluated,
    MissingEvidence,
    EvidenceUnavailable,
    InvalidEvidence,
    HandlerUnavailable,
    PolicyChanged,
    ExecutionError,
    Panicked,
    TimedOut,
    Indeterminate,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerOutcome {
    pub value: VerdictValue,
    pub code: DiagnosticCode,
    pub reason: String,
}
impl WorkerOutcome {
    pub fn error(code: DiagnosticCode, reason: impl Into<String>) -> Self {
        Self {
            value: VerdictValue::Error,
            code,
            reason: reason.into(),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Assignment {
    pub schema: u16,
    pub ledger: LedgerId,
    pub run: ValidationRunId,
    pub claim: ClaimId,
    pub receipt: Option<ReceiptFence>,
    pub evaluator: ParticipantId,
    pub handler: HandlerRef,
    pub attempt: u32,
    pub manifest: ContentHash,
    pub evidence: Vec<ArtifactRef>,
    pub quality_bar: Option<String>,
    pub policy: Option<ExecutionPolicy>,
    pub started_at: u64,
    pub deadline: u64,
}
#[derive(Debug, Clone)]
pub struct EvidenceArtifact {
    pub reference: ArtifactRef,
    pub schema: ContentHash,
    pub payload: ArtifactPayload,
}
#[derive(Debug, Clone)]
pub struct Task {
    pub id: ArtifactId,
    pub assignment: Assignment,
    pub evidence: Vec<EvidenceArtifact>,
    /// True only if this owner just durably created the dispatch record.
    pub fresh_assignment: bool,
    pub max_evidence_bytes: usize,
}
#[derive(Clone)]
// Owner-to-worker cancellation is genuinely concurrent; neither borrowing nor
// single-thread reference counting can outlive the owner's nonblocking Drop.
pub struct Cancellation(Arc<AtomicBool>);
impl Cancellation {
    pub(crate) fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
    pub(crate) fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reconciliation {
    NotStarted,
    Completed(WorkerOutcome),
    Indeterminate,
}
pub trait Executor: Send + Sync {
    fn execute(&self, task: &Task, cancellation: &Cancellation) -> WorkerOutcome;
    fn reconcile(&self, _task: &Task, _cancellation: &Cancellation) -> Reconciliation {
        Reconciliation::Indeterminate
    }
}
/// Reads authenticated immutable bytes off the owner thread. The returned vector
/// must respect max_bytes. Implementations must honor cancellation or use bounded
/// IO timeouts; the coordinator never replaces a still-running blocked worker.
pub trait EvidenceReader: Send + Sync {
    fn read(
        &self,
        content: &ContentRef,
        max_bytes: usize,
        cancellation: &Cancellation,
    ) -> Result<Vec<u8>, DiagnosticCode>;
}
pub struct InlineOnly;
impl EvidenceReader for InlineOnly {
    fn read(
        &self,
        _content: &ContentRef,
        _max: usize,
        _cancel: &Cancellation,
    ) -> Result<Vec<u8>, DiagnosticCode> {
        Err(DiagnosticCode::EvidenceUnavailable)
    }
}
// Immutable registry and reader are shared by concurrently executing workers.
pub struct RegistryExecutor {
    catalog: Arc<Catalog>,
    reader: Arc<dyn EvidenceReader>,
}
impl RegistryExecutor {
    pub fn new(catalog: Arc<Catalog>, reader: Arc<dyn EvidenceReader>) -> Self {
        Self { catalog, reader }
    }
}
impl Executor for RegistryExecutor {
    fn execute(&self, task: &Task, cancellation: &Cancellation) -> WorkerOutcome {
        if cancellation.is_cancelled() {
            return WorkerOutcome::error(DiagnosticCode::TimedOut, "execution was cancelled");
        }
        let Some((registration, _)) = self.catalog.registration(&task.assignment.handler) else {
            return WorkerOutcome::error(
                DiagnosticCode::HandlerUnavailable,
                "pinned handler is unavailable",
            );
        };
        let Some(evidence) = task
            .evidence
            .iter()
            .find(|artifact| artifact.schema == registration.evidence_schema)
        else {
            return WorkerOutcome {
                value: VerdictValue::Incomplete,
                code: DiagnosticCode::MissingEvidence,
                reason: "no artifact matches the pinned handler evidence schema".into(),
            };
        };
        let max = task.max_evidence_bytes.min(registration.max_evidence_bytes);
        let bytes = match &evidence.payload {
            ArtifactPayload::Inline(bytes) if bytes.len() <= max => bytes.clone(),
            ArtifactPayload::Inline(_) => {
                return WorkerOutcome::error(
                    DiagnosticCode::InvalidEvidence,
                    "inline evidence exceeds handler bound",
                );
            }
            ArtifactPayload::Content(reference) => {
                if reference.length > max as u64 {
                    return WorkerOutcome::error(
                        DiagnosticCode::InvalidEvidence,
                        "content exceeds handler evidence bound",
                    );
                }
                match self.reader.read(reference, max, cancellation) {
                    Ok(bytes) if bytes.len() == reference.length as usize && bytes.len() <= max => {
                        bytes
                    }
                    Ok(_) => {
                        return WorkerOutcome::error(
                            DiagnosticCode::InvalidEvidence,
                            "content reader returned wrong length",
                        );
                    }
                    Err(code) => {
                        return WorkerOutcome::error(code, "immutable evidence read failed");
                    }
                }
            }
        };
        if cancellation.is_cancelled() {
            return WorkerOutcome::error(
                DiagnosticCode::TimedOut,
                "execution cancelled before handler",
            );
        }
        match self.catalog.registry.execute(
            &task.assignment.handler,
            registration.evidence_schema,
            &bytes,
            task.assignment.quality_bar.as_deref(),
        ) {
            Ok(Evaluation { value, reason }) => WorkerOutcome {
                value,
                code: DiagnosticCode::Evaluated,
                reason,
            },
            Err(RegistryError::Unavailable) => WorkerOutcome::error(
                DiagnosticCode::HandlerUnavailable,
                "pinned handler is unavailable",
            ),
            Err(RegistryError::Contract) => {
                WorkerOutcome::error(DiagnosticCode::InvalidEvidence, "handler contract mismatch")
            }
            Err(error) => WorkerOutcome::error(DiagnosticCode::ExecutionError, error.to_string()),
        }
    }
}

/// Concrete bounded reader for the local durable content store. A shared write
/// lock lets the node upload concurrently between chunk reads; no ledger-owner
/// lock is held while validators read or execute.
// Shared ownership is required by reader workers and the upload/seal owner.
pub struct SharedStoreReader {
    store: Arc<std::sync::RwLock<focal_evidence::ContentStore>>,
    chunk_bytes: usize,
}
impl SharedStoreReader {
    pub fn new(
        store: Arc<std::sync::RwLock<focal_evidence::ContentStore>>,
        chunk_bytes: usize,
    ) -> Result<Self, RuntimeError> {
        if chunk_bytes == 0 {
            return Err(RuntimeError::Configuration("zero content read chunk"));
        }
        Ok(Self { store, chunk_bytes })
    }
}
impl EvidenceReader for SharedStoreReader {
    fn read(
        &self,
        content: &ContentRef,
        max_bytes: usize,
        cancellation: &Cancellation,
    ) -> Result<Vec<u8>, DiagnosticCode> {
        if content.length > max_bytes as u64 {
            return Err(DiagnosticCode::InvalidEvidence);
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(content.length as usize)
            .map_err(|_| DiagnosticCode::EvidenceUnavailable)?;
        while bytes.len() < content.length as usize {
            if cancellation.is_cancelled() {
                return Err(DiagnosticCode::TimedOut);
            }
            let remaining = usize::try_from(content.length)
                .map_err(|_| DiagnosticCode::EvidenceUnavailable)?
                .checked_sub(bytes.len())
                .ok_or(DiagnosticCode::EvidenceUnavailable)?;
            let chunk = self
                .store
                .read()
                .map_err(|_| DiagnosticCode::EvidenceUnavailable)?
                .read_range(content, bytes.len() as u64, remaining.min(self.chunk_bytes))
                .map_err(|error| {
                    if matches!(error, focal_evidence::ContentError::Corrupt) {
                        DiagnosticCode::InvalidEvidence
                    } else {
                        DiagnosticCode::EvidenceUnavailable
                    }
                })?;
            if chunk.is_empty() || chunk.len() > remaining {
                return Err(DiagnosticCode::InvalidEvidence);
            }
            bytes.extend(chunk);
        }
        Ok(bytes)
    }
}

impl Task {
    /// Stable external idempotency/fencing key for this logical validation attempt.
    /// Receipt adoption and owner restart do not create a second external effect.
    /// Effectful adapters must bind this key atomically in the external system;
    /// a non-atomic "not found" query alone cannot guarantee safe execution.
    pub fn effect_key(&self) -> ContentHash {
        let assignment = &self.assignment;
        let mut hash = blake3::Hasher::new_derive_key("focal.runtime.external-effect.v1");
        hash.update(&assignment.ledger.tenant.0);
        hash.update(&assignment.ledger.session.0);
        hash.update(&assignment.run.validation.0);
        hash.update(&assignment.run.target_hash.0);
        hash.update(&assignment.run.phase.code().to_be_bytes());
        hash.update(&assignment.run.epoch.to_be_bytes());
        hash.update(&assignment.attempt.to_be_bytes());
        hash.update(&assignment.handler.id.0);
        hash.update(&assignment.handler.version.0);
        hash.update(&[u8::from(assignment.handler.agentic)]);
        hash.update(&assignment.manifest.0);
        hash.update(&assignment.evaluator.0);
        ContentHash(*hash.finalize().as_bytes())
    }
}
