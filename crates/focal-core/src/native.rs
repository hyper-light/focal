//! Sole owner for native lifecycle rows. This is a typed, in-process boundary;
//! Session activation and a qualified native durable codec remain separate work.
//! There is no native serde implementation, arbitrary row insertion, or second
//! mutable representation alongside the legacy Core.
mod admission_authority;
#[cfg(test)]
mod admission_authority_tests;
mod admission_budget;
mod admission_graph;
mod admission_view;
mod adoption;
mod artifact_intent;
mod audit;
mod audit_bundle;
mod authored;
mod authored_reads;
mod claim_changes;
mod claim_deadlines;
mod cohort_budget;
mod cohort_seals;
mod completion_book;
mod completion_envelope;
mod completion_index;
mod completion_schemas;
mod control_graph;
mod creation_result;
mod deadlines;
mod delivery;
mod delivery_owned;
#[cfg(test)]
mod event_budget_tests;
mod event_index;
pub mod event_record;
#[cfg(any(test, feature = "test-support"))]
pub mod fixtures;
#[cfg(test)]
mod funding_tests;
mod graph_effects;
mod history;
mod import;
mod incoming_graph;
mod increment_authority;
mod increment_seal;
mod increments;
mod index_rows;
mod index_scan;
pub mod input_codec;
mod intent;
mod layout;
pub use layout::NativeLocation;
mod epochs;
#[cfg(test)]
mod layout_tests;
mod missing_owned;
mod missing_results;
mod monitor_commands;
mod monitor_deadlines;
mod monitor_index;
mod mutation;
mod object_journal;
mod owned;
mod owner;
#[cfg(test)]
mod owner_record_buffer_tests;
pub mod payloads;
mod pending_index;
mod prepare;
mod prepare_budget;
mod projection;
mod projection_quote;
mod projection_visits;
mod projection_work;
pub mod ranges;
mod read_back;
mod receipt;
#[cfg(test)]
mod receipt_tests;
pub mod record_codec;
mod report_artifact;
#[cfg(test)]
mod report_tests;
mod reporting;
mod respondent_envelope;
mod respondent_state;
mod response_budget;
mod response_input;
mod response_owned;
mod response_reads;
#[cfg(test)]
mod response_tests;
mod responses;
mod result_owned;
mod retired_cycles;
pub mod retirement;
pub mod retirement_batch;
mod young;
pub use epochs::{EpochWindow, OPEN_EPOCHS, OpenEpoch, SealedRange};
pub mod seal;
pub use seal::SealRow;
#[cfg(test)]
#[path = "native/retirement_batch_tests.rs"]
mod retirement_batch_tests;
#[cfg(test)]
#[path = "native/retirement_tests.rs"]
mod retirement_tests;
mod scope_release;
#[cfg(test)]
mod scope_release_tests;
#[cfg(test)]
#[path = "native/seal_tests.rs"]
mod seal_tests;
#[cfg(test)]
mod tests;
mod transactions;
#[cfg(test)]
mod validation_tests;
mod whole_work;
mod work_artifacts;
mod work_authority;
mod work_checks;
mod work_failures;
mod work_owned;
mod work_reporting;

use crate::{Core, CoreState, state_kind};
pub use audit::{NativeAudit, NativeAuditPublication};
pub use audit_bundle::NativeResultTestament;
use audit_bundle::OwnedResultTestament;
pub use authored::{NativeAuthoredProposal, NativeContentProfile};
use creation_result::OwnedCreationResult;
pub use creation_result::{
    NativeCreatedFamily, NativeCreatedObject, NativeCreationRead, NativeCreationResult,
};
pub use delivery_owned::NativeDeliveryResult;
use delivery_owned::OwnedDeliveryResult;
pub use event_record::delta as native_delta;
use focal_memory::{
    BudgetStats, MemoryBudget, MemoryError, RangeConfig, RangeId, RangeStats, RangeStore,
};
use focal_model::lifecycle::evidence::{
    self, EvidenceFailure, Response, ResponseState, WorkArtifactState,
};
use focal_model::lifecycle::{
    Binding, ContractError, Principal,
    aggregation::RegistrationSet,
    claim::{ClaimState, ReceiptEntitlement},
    creation::{EffectiveClaims, Proposal},
    scope, validation,
};
use focal_model::{
    ArtifactId, ClaimId, ClaimStatus, ContentHash, Deadline, LedgerId, MonitorId, ParticipantId,
    ReceiptFence, ReceiptId, RequestEpoch, RequestId, RequestKey, SessionSeq, TestamentId, TimerId,
    ValidationId, WaitPredicate,
};
pub use import::{ImportError, ImportRequest, Imported, InlinePayload, import, inline_payloads};
pub use index_rows::{artifact_kind_hash, scope_key_hash};
pub use index_scan::{NativeIndexHit, NativeIndexScan};
pub use missing_owned::NativeMissingResult;
use missing_owned::OwnedMissingResult;
use owned::{
    OwnedClaim, OwnedClaimContent, OwnedDeclaration, OwnedEvaluation, OwnedEvent, OwnedLegacy,
};
pub use owner::{
    CheckpointProjection, NativeCandidate, NativeOwner, NativeOwnerError, NativeOwnerInitError,
    NativeOwnerIntoCoreError, NativeStaging, NativeView,
};
pub use projection_quote::NativeProjectionQuote;
/// The least memory one row of a native root holds resident: its key and its
/// row, inline. A root charged to a budget of `n` bytes holds at most
/// `n / NATIVE_ENTRY_BYTES` rows, which is the most a session's checkpoint
/// must be able to carry.
pub const NATIVE_ENTRY_BYTES: usize = size_of::<(Key, Row)>();
pub use index_rows::ContentSource as NativeClaimContentRead;
pub use owned::HeldClaimBody;
pub use response_input::{
    NativeMonitorSource, NativeMonitorSourcePlan, NativeResponseInput, NativeResponsePlan,
    NativeResponseSource, NativeResponseSourcePlan, NativeResponseSpec, NativeSourceQuote,
};
pub use response_owned::NativeResponseRecord;
use response_owned::OwnedResponse;
use response_reads::{as_diagnostic, as_response, as_work};
pub use result_owned::{NativeAccepted, NativeArtifact, NativeArtifactInput};
use result_owned::{OwnedAccepted, OwnedArtifact};
pub use work_owned::{NativeDiagnostic, NativeWork};
use work_owned::{OwnedDiagnostic, OwnedWork};

/// Internal resource limits. Deployment profiles derive these from the node's
/// allowance; they are not another set of mandatory end-user configuration.
#[derive(Debug, Clone, Copy)]
pub struct NativeLimits {
    pub range: RangeConfig,
    pub pending: usize,
    pub plan_nodes: usize,
    pub plan_edges: usize,
    pub preparation_bytes: usize,
    pub claims: usize,
    /// Resident outcomes: the open obligations and the unsealed tail, never
    /// the ledger's lifetime (F12).
    pub outcomes: usize,
    /// Principals with a request generation window: the enrollment bound.
    pub principals: usize,
    /// Resident seal rows; at the bound the oldest half fold into one.
    pub seals: usize,
    pub events: usize,
    pub definitions: usize,
    pub evaluations: usize,
    pub evaluations_per_claim: usize,
    pub artifacts: usize,
    pub results: usize,
    pub receipts: usize,
    pub responses: usize,
    pub monitors: usize,
    pub monitor_links: usize,
    pub work_artifacts_per_cycle: usize,
    pub diagnostics_per_cycle: usize,
    pub response_summary_bytes: usize,
    /// The guaranteed inline size of a completion/respondent report or
    /// diagnostic artifact descriptor the owner pre-reserves per outstanding
    /// receipt (doc 18 §6.7). This decouples the per-respondent completion
    /// promise from `preparation_bytes`: inline payloads up to this bound are
    /// guaranteed, and larger artifact content rides the separately bounded
    /// content store as a `Payload::Content` pointer rather than pinning owner
    /// RAM. Keeping it small is what lets one node hold many concurrent
    /// respondents (the per-node concurrency envelope that sizes the ledger).
    pub report_descriptor_bytes: usize,
    /// Inputs one artifact may cite, and so the `ArtifactInput` index rows its
    /// admission writes (22 §7); never above the model's fixed ceiling.
    pub artifact_inputs: usize,
    /// Frozen legacy rows an imported ledger may retain (23 §5.2).
    pub legacy_rows: usize,
    pub legacy_row_bytes: usize,
    /// Members a range group may have (25 §4): every write envelope is
    /// derived for this many, so splits never invalidate a promise.
    pub max_ranges: usize,
}
impl Default for NativeLimits {
    fn default() -> Self {
        Self {
            range: RangeConfig::default(),
            pending: 32,
            plan_nodes: 256,
            plan_edges: 4096,
            preparation_bytes: 4 * 1024 * 1024,
            claims: 1_000_000,
            outcomes: 1_000_000,
            principals: 4096,
            seals: seal::DEFAULT_SEAL_ROWS,
            events: 16_000_000,
            definitions: 4_000_000,
            evaluations: 8_000_000,
            evaluations_per_claim: 4096,
            artifacts: 8_000_000,
            results: 8_000_000,
            receipts: 8_000_000,
            responses: 4_000_000,
            monitors: 4_000_000,
            monitor_links: 16_000_000,
            work_artifacts_per_cycle: 256,
            diagnostics_per_cycle: 64,
            response_summary_bytes: 64 * 1024,
            report_descriptor_bytes: 128 * 1024,
            artifact_inputs: 16,
            legacy_rows: 4_000_000,
            legacy_row_bytes: 1024 * 1024,
            max_ranges: 64,
        }
    }
}

pub struct NativeState {
    ledger: LedgerId,
    profile: NativeContentProfile,
    rows: ranges::NativeRanges,
    budget: MemoryBudget,
    /// What the rows take in a checkpoint, exactly (`record_codec::checkpoint::row_bytes`
    /// summed): kept as each mutation publishes, and summed once where a root is built whole.
    /// What admission projects a checkpoint to is this and the frame, never a bound inferred
    /// from the memory the rows are charged.
    encoded_rows: usize,
    /// Each object's events by where they were published (doc 31 M2): derived from the rows,
    /// never encoded, kept as each publication commits.
    events: event_index::EventIndex,
    /// The replica's payload log, where a row written once and read rarely is spilled where it
    /// is built (doc 31 M3). A core without one keeps every row's bytes.
    payloads: Option<Box<dyn payloads::PayloadLog>>,
    /// The spilled claims and definitions reads have taken back since the state last changed
    /// (doc 31 M3).
    reads: read_back::ReadBack,
    /// The newest spillable claims and definitions, held until newer ones push them out
    /// (doc 31 M3, [`young`]).
    young: young::Young,
}
impl NativeState {
    /// Where a spilled row's bytes are read from.
    pub(crate) fn payloads(&self) -> Option<&dyn payloads::PayloadRead> {
        self.payloads
            .as_deref()
            .map(|log| -> &dyn payloads::PayloadRead { log })
    }
    /// The committed row at `key` as a read takes it: a spilled claim or definition read back
    /// (doc 31 M3).
    fn row(&self, key: &Key) -> Option<&Row> {
        let stored = self.rows.get(key)?;
        Some(self.read_back(&self.reads, key, stored, &StateRows(self, &self.reads)))
    }
    /// `stored`, the row at `key` of a state `rows` reads, as a read takes it into `reads`: a
    /// spilled claim or definition read back, else itself. One that cannot be read back is lent
    /// as it is stored, which holds nothing a reader can take, and its failure kept in `reads`
    /// for the operation ([`read_back::ReadBack::failure`]).
    fn read_back<'s, R: record_codec::ClaimRows + ?Sized>(
        &'s self,
        reads: &'s read_back::ReadBack,
        key: &Key,
        stored: &'s Row,
        rows: &R,
    ) -> &'s Row {
        let read = match stored {
            Row::Claim(claim) => match (key, claim.logged()) {
                (Key::Claim(id), Some(at)) => self.read_back_claim(reads, *id, at, rows),
                (_, Some(_)) => Err(ContractError::InvalidManifest.into()),
                (_, None) => return stored,
            },
            Row::Definition(definition) => match definition.logged() {
                Some(at) => self.read_back_definition(reads, at),
                None => return stored,
            },
            _ => return stored,
        };
        match read {
            Ok(row) => row,
            Err(error) => {
                reads.fail(error);
                stored
            }
        }
    }
    fn read_back_claim<'s, R: record_codec::ClaimRows + ?Sized>(
        &'s self,
        reads: &'s read_back::ReadBack,
        id: ClaimId,
        at: payloads::PayloadRef,
        rows: &R,
    ) -> Result<&'s Row, NativeError> {
        if let Some(row) = reads.find(at) {
            return Ok(row);
        }
        let fund = |bytes| reads.fund(&self.budget, bytes);
        let (claim, charge) = self.with_spilled(at, |bytes| {
            record_codec::decode_claim(id, bytes, self.ledger, rows, reads.limits(), &fund)
        })?;
        reads.hold(&self.budget, at, Row::Claim(claim), charge)
    }
    fn read_back_definition<'s>(
        &'s self,
        reads: &'s read_back::ReadBack,
        at: payloads::PayloadRef,
    ) -> Result<&'s Row, NativeError> {
        read_back_definition(reads, self.payloads(), &self.budget, at)
    }
    /// The spilled bytes at `at`, read into a stack buffer and handed to `read`.
    fn with_spilled<T>(
        &self,
        at: payloads::PayloadRef,
        read: impl FnOnce(&[u8]) -> Result<T, NativeError>,
    ) -> Result<T, NativeError> {
        with_spilled(self.payloads(), at, read)
    }
    /// A definition at `key` of the committed rows, a spilled one read back into `reads`.
    fn stored_definition<'s>(
        &'s self,
        reads: &'s read_back::ReadBack,
        id: ValidationId,
    ) -> Option<&'s validation::Declaration> {
        let stored = self.rows.get(&Key::Definition(id))?;
        as_definition(Some(self.read_back_held_definition(reads, stored)))
    }
    /// `stored` a definition's row as a read takes it into `reads`, without reading a claim back.
    fn read_back_held_definition<'s>(
        &'s self,
        reads: &'s read_back::ReadBack,
        stored: &'s Row,
    ) -> &'s Row {
        let Row::Definition(definition) = stored else {
            return stored;
        };
        let Some(at) = definition.logged() else {
            return stored;
        };
        match self.read_back_definition(reads, at) {
            Ok(row) => row,
            Err(error) => {
                reads.fail(error);
                stored
            }
        }
    }
}
/// The spilled bytes at `at` in `payloads`, read into a stack buffer and handed to `read`.
fn with_spilled<T>(
    payloads: Option<&dyn payloads::PayloadRead>,
    at: payloads::PayloadRef,
    read: impl FnOnce(&[u8]) -> Result<T, NativeError>,
) -> Result<T, NativeError> {
    let mut buffer = [0u8; payloads::SPILLED_BYTES];
    let bytes = buffer
        .get_mut(..at.len())
        .ok_or(payloads::PayloadError::Corrupt)?;
    payloads
        .ok_or(payloads::PayloadError::Unavailable)?
        .read(at, bytes)?;
    read(bytes)
}
/// The definition spilled at `at` in `payloads`, read back into `reads` and charged to `budget`.
fn read_back_definition<'s>(
    reads: &'s read_back::ReadBack,
    payloads: Option<&dyn payloads::PayloadRead>,
    budget: &MemoryBudget,
    at: payloads::PayloadRef,
) -> Result<&'s Row, NativeError> {
    if let Some(row) = reads.find(at) {
        return Ok(row);
    }
    let fund = |bytes| reads.fund(budget, bytes);
    let (definition, charge) = with_spilled(payloads, at, |bytes| {
        record_codec::decode_definition(bytes, &fund)
    })?;
    reads.hold(budget, at, Row::Definition(definition), charge)
}
/// A read back through a lease that failed: its memory refusal, or a row that could not be.
fn lease_read_back(error: NativeError) -> MemoryError {
    match error {
        NativeError::Memory(error) => error,
        _ => MemoryError::InvalidConfiguration("a spilled row could not be read back"),
    }
}
/// The rows a spilled claim read through a lease names, read in the same leased prefix, a
/// spilled definition read back into the read's own arena.
struct LeaseRows<'r> {
    rows: &'r ranges::LeasedRows<'r>,
    reads: &'r read_back::ReadBack,
    payloads: Option<&'r dyn payloads::PayloadRead>,
    budget: &'r MemoryBudget,
}
impl LeaseRows<'_> {
    fn definition(&self, id: ValidationId) -> Option<&Row> {
        let stored = self.rows.get(&Key::Definition(id))?;
        match stored {
            Row::Definition(definition) => match definition.logged() {
                Some(at) => read_back_definition(self.reads, self.payloads, self.budget, at).ok(),
                None => Some(stored),
            },
            _ => Some(stored),
        }
    }
}
impl record_codec::ClaimRows for LeaseRows<'_> {
    fn declaration(&self, id: ValidationId) -> Option<&validation::Declaration> {
        as_definition(self.definition(id))
    }
    fn response(&self, id: TestamentId) -> Option<&focal_model::lifecycle::evidence::Response> {
        match self.rows.get(&Key::Response(id)) {
            Some(Row::Response(row)) => row.get(),
            _ => None,
        }
    }
    fn evaluation(&self, key: EvaluationKey) -> Option<&validation::EvaluationState> {
        as_evaluation(self.rows.get(&Key::Evaluation(key)))
    }
}
/// A new core's payload log: none, but in tests run with `FOCAL_TEST_SPILL` set, where every
/// core spills to one held in memory, so every reader meets the rows spilled (doc 31 M3).
#[cfg(not(test))]
fn test_payloads() -> Option<Box<dyn payloads::PayloadLog>> {
    None
}
#[cfg(test)]
fn test_payloads() -> Option<Box<dyn payloads::PayloadLog>> {
    std::env::var_os("FOCAL_TEST_SPILL")
        .map(|_| -> Box<dyn payloads::PayloadLog> { Box::new(payloads::MemoryPayloads::default()) })
}
/// The rows a spilled claim of the committed rows names, read there, a spilled definition read
/// back into the same reads.
struct StateRows<'s>(&'s NativeState, &'s read_back::ReadBack);
impl record_codec::ClaimRows for StateRows<'_> {
    fn declaration(&self, id: ValidationId) -> Option<&validation::Declaration> {
        self.0.stored_definition(self.1, id)
    }
    fn response(&self, id: TestamentId) -> Option<&focal_model::lifecycle::evidence::Response> {
        match self.0.rows.get(&Key::Response(id)) {
            Some(Row::Response(row)) => row.get(),
            _ => None,
        }
    }
    fn evaluation(&self, key: EvaluationKey) -> Option<&validation::EvaluationState> {
        as_evaluation(self.0.rows.get(&Key::Evaluation(key)))
    }
}
impl std::fmt::Debug for NativeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeState")
            .field("ledger", &self.ledger)
            .field("profile", &self.profile)
            .field("range", &self.rows.id())
            .field("members", &self.rows.members())
            .field("prefix", &self.rows.prefix())
            .field("entries", &self.rows.len())
            .finish_non_exhaustive()
    }
}
impl state_kind::Sealed for NativeState {}
impl CoreState for NativeState {
    type Limits = NativeLimits;
}

#[derive(Debug)]
pub struct NativeInput {
    pub request: RequestKey,
    pub command: NativeCommand,
}
/// Trusted timer delivery. The owner resolves the current evaluation revision;
/// this is deliberately separate from participant-authored commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeDeadlineInput {
    pub evaluation: EvaluationKey,
    pub deadline: Deadline,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct NativeDeadlineKey {
    pub evaluation: EvaluationKey,
    pub timer: TimerId,
    pub generation: u64,
}
impl NativeDeadlineInput {
    pub fn key(self) -> NativeDeadlineKey {
        NativeDeadlineKey {
            evaluation: self.evaluation,
            timer: self.deadline.timer,
            generation: self.deadline.generation,
        }
    }
}
/// Trusted delivery of the claim's own authored deadline. The effective owner
/// resolves its current revision and checks deadlock precedence before expiry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeClaimDeadlineInput {
    pub claim: ClaimId,
    pub deadline: Deadline,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct NativeClaimDeadlineKey {
    pub claim: ClaimId,
    pub timer: TimerId,
    pub generation: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeMonitorDeadlineInput {
    pub claim: ClaimId,
    pub monitor: MonitorId,
    pub deadline: Deadline,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct NativeMonitorDeadlineKey {
    pub claim: ClaimId,
    pub monitor: MonitorId,
    pub timer: TimerId,
    pub generation: u64,
}
impl NativeMonitorDeadlineInput {
    pub fn key(self) -> NativeMonitorDeadlineKey {
        NativeMonitorDeadlineKey {
            claim: self.claim,
            monitor: self.monitor,
            timer: self.deadline.timer,
            generation: self.deadline.generation,
        }
    }
}
impl NativeClaimDeadlineInput {
    pub fn key(self) -> NativeClaimDeadlineKey {
        NativeClaimDeadlineKey {
            claim: self.claim,
            timer: self.deadline.timer,
            generation: self.deadline.generation,
        }
    }
}
/// Disjoint exact-invocation namespaces. A timer can never impersonate an
/// actor request or consume its retry identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum NativeInvocation {
    Request(RequestKey),
    EvaluationDeadline(NativeDeadlineKey),
    ClaimDeadline(NativeClaimDeadlineKey),
    MonitorDeadline(NativeMonitorDeadlineKey),
    /// The one-time translation of a sealed legacy prefix (23 §5). Never a
    /// participant request and never a timer; it owns native sequence one.
    Import,
    /// A committed retirement (26 §4) that moved the family rooted at the
    /// named claim to the archive; a session decision, never a request or
    /// a timer, owning the native sequence its publication consumed.
    Retirement(ClaimId),
    /// A committed seal of closed outcomes (F12), by its ordinal: a session
    /// decision owning the native sequence its publication consumed.
    Seal(u64),
}
/// Custody request identity of an imported artifact (23 §5.2): derived from
/// the ledger and artifact under a private domain, principal = producer, epoch
/// one. It is never minted by a participant and never consumes a retry slot.
pub fn import_request(
    ledger: LedgerId,
    artifact: ArtifactId,
    producer: ParticipantId,
) -> RequestKey {
    let mut hash = blake3::Hasher::new_derive_key("focal.native.import.request.v1");
    hash.update(&ledger.tenant.0);
    hash.update(&ledger.session.0);
    hash.update(&artifact.0);
    let mut id = [0u8; 16];
    hash.finalize_xof().fill(&mut id);
    RequestKey {
        principal: producer,
        epoch: RequestEpoch(1),
        id: RequestId(id),
    }
}
impl From<RequestKey> for NativeInvocation {
    fn from(request: RequestKey) -> Self {
        Self::Request(request)
    }
}
impl From<NativeDeadlineKey> for NativeInvocation {
    fn from(deadline: NativeDeadlineKey) -> Self {
        Self::EvaluationDeadline(deadline)
    }
}
impl From<NativeClaimDeadlineKey> for NativeInvocation {
    fn from(deadline: NativeClaimDeadlineKey) -> Self {
        Self::ClaimDeadline(deadline)
    }
}
impl From<NativeMonitorDeadlineKey> for NativeInvocation {
    fn from(deadline: NativeMonitorDeadlineKey) -> Self {
        Self::MonitorDeadline(deadline)
    }
}
/// Supplied by the trusted publishing owner, never decoded from participant
/// intent. Logical time advances monotonically at the effective ledger prefix.
#[derive(Debug, Clone, Copy)]
pub struct NativeContext {
    pub principal: Principal,
    pub logical_time: u64,
}
#[derive(Debug)]
pub enum NativeCommand {
    RegisterMonitor {
        expected: Binding,
        receipt: Option<ReceiptFence>,
        id: MonitorId,
        roots: Vec<WaitPredicate>,
        deadline: Deadline,
    },
    RebindMonitor {
        expected: Binding,
        receipt: Option<ReceiptFence>,
        id: MonitorId,
        predecessor: Binding,
        successor: Binding,
    },
    CancelMonitor {
        expected: Binding,
        receipt: Option<ReceiptFence>,
        id: MonitorId,
    },
    /// Release a terminal claim's owned scope after its children are released.
    /// This does not terminalize children or invent evaluation results.
    ReleaseScope {
        expected: Binding,
    },
    /// Advance the principal's own request generation floor (F12): the
    /// generations below `minimum` close; their outcomes leave for a seal.
    AdvanceEpochFloor {
        minimum: RequestEpoch,
    },
    GenerateResultTestament {
        claim: Binding,
        id: TestamentId,
    },
    PostResultTestament {
        expected: Binding,
    },
    /// Explicit claimant request to assess the exact received response.
    /// External check execution remains the evaluator's responsibility.
    EnterWholeWork {
        claim: Binding,
        expected: Binding,
    },
    /// Claimant freezes Increment target membership before WholeWork entry.
    /// Existing evaluations retain their independent completion authority.
    SealIncrementTargets {
        claim: Binding,
    },
    FailWorkProduction {
        claim: Binding,
        slot: u32,
        diagnostic: focal_model::ArtifactRef,
    },
    RejectWork {
        claim: Binding,
        expected: Binding,
        reason: EvidenceFailure,
        artifact: NativeArtifactInput,
    },
    SubmitWork {
        claim: Binding,
        slot: u32,
        artifact: NativeArtifactInput,
    },
    SubmitDiagnostic {
        claim: Binding,
        reason: EvidenceFailure,
        artifact: NativeArtifactInput,
    },
    ReceiveWork {
        claim: Binding,
        expected: Binding,
    },
    CloseResponse {
        claim: Binding,
        response: Binding,
        report: NativeResponseInput,
    },
    PostResponse {
        claim: Binding,
        expected: Binding,
    },
    ReceiveResponse {
        claim: Binding,
        expected: Binding,
    },
    /// First responsibility receipt. The owner assigns epoch one and retains the
    /// unique ID allocation even after later authority changes.
    AcquireReceipt {
        expected: Binding,
        receipt: ReceiptId,
    },
    /// The claimant replaces current responsibility. The owner assigns the next
    /// receipt epoch and fences old evaluations without manufacturing testimony.
    AdoptReceipt {
        expected: Binding,
        previous: ReceiptFence,
        receipt: ReceiptId,
        holder: ParticipantId,
    },
    /// Core assigns every creation position. Definitions must begin at revision
    /// one; their immutable content and acceptance identities remain pinned.
    Create {
        claims: Vec<Proposal>,
        declarations: Vec<validation::Declaration>,
    },
    /// Complete authored bodies, grouped by their actual parent. The owner
    /// derives lifecycle projections and assigns the creation cut.
    CreateAuthored {
        claims: Vec<NativeAuthoredProposal>,
    },
    /// Root-issuer control of the actual stored owned closure, including children
    /// created in an earlier unpublished candidate in the supplied chain.
    Cancel {
        expected: Binding,
    },
    Post {
        expected: Binding,
    },
    BeginAdmission {
        claim: Binding,
        key: EvaluationKey,
        expected: Binding,
    },
    BeginIncrement {
        claim: Binding,
        key: EvaluationKey,
        expected: Binding,
    },
    ReportIncrement {
        claim: Binding,
        key: EvaluationKey,
        expected: Binding,
        report: validation::Report,
        artifact: NativeArtifactInput,
    },
    ReportAdmission {
        claim: Binding,
        key: EvaluationKey,
        expected: Binding,
        report: validation::Report,
        artifact: NativeArtifactInput,
    },
    /// The designated evaluator begins one exact received-work check. The first
    /// begin enters its response using the actual evaluator capability.
    BeginWork {
        claim: Binding,
        key: EvaluationKey,
        expected: Binding,
    },
    ReportWork {
        claim: Binding,
        key: EvaluationKey,
        expected: Binding,
        report: validation::Report,
        artifact: NativeArtifactInput,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeOperation {
    RegisterMonitor,
    RebindMonitor,
    CancelMonitor,
    MonitorDeadline,
    ReleaseScope,
    GenerateResultTestament,
    PostResultTestament,
    EnterWholeWork,
    SealIncrementTargets,
    BeginIncrement,
    ReportIncrement,
    FailWorkProduction,
    RejectWork,
    SubmitWork,
    SubmitDiagnostic,
    ReceiveWork,
    CloseResponse,
    PostResponse,
    ReceiveResponse,
    AcquireReceipt,
    AdoptReceipt,
    Create,
    Cancel,
    Post,
    BeginAdmission,
    ReportAdmission,
    BeginWork,
    ReportWork,
    EvaluationDeadline,
    ClaimDeadline,
    Import,
    /// A family of claims retired to the archive (26 §4).
    Retire,
    /// A principal advanced its request generation floor (F12).
    AdvanceEpochFloor,
    /// Closed outcomes sealed into a bundle (F12): a session decision.
    Seal,
}
/// Address of one frozen legacy row retained by import.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeLegacyKey {
    Testament(TestamentId),
    EvidenceSet(focal_model::EvidenceSetId),
    Run { validation: ValidationId, run: u32 },
    Definition(ValidationId),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeOutcome {
    pub ledger: LedgerId,
    pub invocation: NativeInvocation,
    pub sequence: SessionSeq,
    pub logical_time: u64,
    pub operation: NativeOperation,
    /// Private native semantic intent identity; not a V1 hash or a wire codec.
    pub intent: ContentHash,
    pub created: u32,
    pub changed: u32,
    pub definitions: u32,
    pub evaluations: u32,
    pub artifacts: u32,
    pub results: u32,
    pub receipts: u32,
    pub responses: u32,
    pub result_testaments: u32,
    pub events: u32,
}
/// An outcome as its row holds it: the ledger is its store's and the invocation is its row's key, so the row
/// keeps neither. A whole outcome is 232 bytes and set the size of every row of every family (a unit index
/// row among them); this is under half of it. The record and checkpoint bytes are unchanged: the codec
/// writes the whole outcome, expanded from the key and the store's ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct OutcomeRow {
    pub(super) sequence: SessionSeq,
    pub(super) logical_time: u64,
    pub(super) operation: NativeOperation,
    pub(super) intent: ContentHash,
    pub(super) created: u32,
    pub(super) changed: u32,
    pub(super) definitions: u32,
    pub(super) evaluations: u32,
    pub(super) artifacts: u32,
    pub(super) results: u32,
    pub(super) receipts: u32,
    pub(super) responses: u32,
    pub(super) result_testaments: u32,
    pub(super) events: u32,
}
impl OutcomeRow {
    /// The row of `outcome` in a store of `ledger`; an outcome of another ledger is refused, never kept as
    /// this one's.
    pub(super) fn stored(outcome: &NativeOutcome, ledger: LedgerId) -> Result<Self, ContractError> {
        if outcome.ledger != ledger {
            return Err(ContractError::WrongLedger);
        }
        Ok(Self {
            sequence: outcome.sequence,
            logical_time: outcome.logical_time,
            operation: outcome.operation,
            intent: outcome.intent,
            created: outcome.created,
            changed: outcome.changed,
            definitions: outcome.definitions,
            evaluations: outcome.evaluations,
            artifacts: outcome.artifacts,
            results: outcome.results,
            receipts: outcome.receipts,
            responses: outcome.responses,
            result_testaments: outcome.result_testaments,
            events: outcome.events,
        })
    }
    /// The whole outcome, from the store's ledger and the row's key.
    pub(super) fn expand_held(
        &self,
        ledger: LedgerId,
        invocation: NativeInvocation,
    ) -> NativeOutcome {
        NativeOutcome {
            ledger,
            invocation,
            sequence: self.sequence,
            logical_time: self.logical_time,
            operation: self.operation,
            intent: self.intent,
            created: self.created,
            changed: self.changed,
            definitions: self.definitions,
            evaluations: self.evaluations,
            artifacts: self.artifacts,
            results: self.results,
            receipts: self.receipts,
            responses: self.responses,
            result_testaments: self.result_testaments,
            events: self.events,
        }
    }
}
/// An outcome as its row holds it (doc 31 M3): boxed, or spilled to the replica's payload log as
/// its record encoding. A spilled one keeps beside where it is the two fields walks across many
/// outcomes read (a seal's sequence and events, a retirement's events), so no walk reads one back,
/// in twenty-four bytes: a row is no wider for it.
#[derive(Debug)]
pub(super) enum OwnedOutcome {
    Held(Box<OutcomeRow>),
    Logged(LoggedOutcome),
}
/// Where a spilled outcome's record encoding is, and the fields its walks read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct LoggedOutcome {
    sequence: SessionSeq,
    segment: u32,
    offset: u32,
    len: u16,
    events: u16,
    check: std::num::NonZeroU32,
}
impl LoggedOutcome {
    pub(super) fn at(self) -> payloads::PayloadRef {
        payloads::PayloadRef::from_parts(
            self.segment,
            self.offset,
            u32::from(self.len),
            self.check.get(),
        )
    }
}
impl OwnedOutcome {
    /// `outcome`'s row in a store of `ledger`, held.
    pub(super) fn stored(outcome: &NativeOutcome, ledger: LedgerId) -> Result<Self, ContractError> {
        Ok(Self::Held(Box::new(OutcomeRow::stored(outcome, ledger)?)))
    }
    pub(super) fn sequence(&self) -> SessionSeq {
        match self {
            Self::Held(row) => row.sequence,
            Self::Logged(at) => at.sequence,
        }
    }
    pub(super) fn events(&self) -> u32 {
        match self {
            Self::Held(row) => row.events,
            Self::Logged(at) => u32::from(at.events),
        }
    }
    /// The whole outcome, from the store's ledger and the row's key, a spilled one read back
    /// through `payloads` and refused unless it is the outcome of that ledger and invocation.
    pub(super) fn expand(
        &self,
        ledger: LedgerId,
        invocation: NativeInvocation,
        payloads: Option<&dyn payloads::PayloadRead>,
    ) -> Result<NativeOutcome, NativeError> {
        match self {
            Self::Held(row) => Ok(row.expand_held(ledger, invocation)),
            Self::Logged(logged) => {
                let at = logged.at();
                let mut buffer = [0u8; payloads::SPILLED_BYTES];
                let bytes = buffer
                    .get_mut(..at.len())
                    .ok_or(payloads::PayloadError::Corrupt)?;
                payloads
                    .ok_or(payloads::PayloadError::Unavailable)?
                    .read(at, bytes)?;
                let outcome = record_codec::decode_outcome(bytes)
                    .map_err(|_| payloads::PayloadError::Corrupt)?;
                if outcome.ledger != ledger
                    || outcome.invocation != invocation
                    || outcome.sequence != logged.sequence
                    || outcome.events != u32::from(logged.events)
                {
                    return Err(payloads::PayloadError::Corrupt.into());
                }
                Ok(outcome)
            }
        }
    }
    /// A held row's fields: none for a spilled one.
    pub(super) fn held(&self) -> Option<&OutcomeRow> {
        match self {
            Self::Held(row) => Some(row),
            Self::Logged(_) => None,
        }
    }
    /// The row's fields, a spilled one read back through `payloads`.
    pub(super) fn row(
        &self,
        ledger: LedgerId,
        invocation: NativeInvocation,
        payloads: Option<&dyn payloads::PayloadRead>,
    ) -> Result<OutcomeRow, NativeError> {
        match self {
            Self::Held(row) => Ok(**row),
            Self::Logged(_) => Ok(OutcomeRow::stored(
                &self.expand(ledger, invocation, payloads)?,
                ledger,
            )?),
        }
    }
    /// This row spilled to `log` as its record encoding, where it is held, the log takes it and
    /// its events fit the spilled row's count: what a candidate's rows become once every check
    /// has read them.
    pub(super) fn spill(
        &self,
        ledger: LedgerId,
        invocation: NativeInvocation,
        log: &dyn payloads::PayloadLog,
    ) -> Result<Option<Self>, NativeError> {
        let Self::Held(row) = self else {
            return Ok(None);
        };
        let Ok(events) = u16::try_from(row.events) else {
            return Ok(None);
        };
        let mut buffer = [0u8; payloads::SPILLED_BYTES];
        let len = record_codec::encode_outcome(row.expand_held(ledger, invocation), &mut buffer)
            .map_err(|_| NativeError::Capacity("outcome row"))?;
        let (Ok(short), Some(bytes)) = (u16::try_from(len), buffer.get(..len)) else {
            return Ok(None);
        };
        let Some(at) = log.append(bytes) else {
            return Ok(None);
        };
        let Some(check) = std::num::NonZeroU32::new(at.check()) else {
            // A check of zero has no spilled form: the row stays held.
            return Ok(None);
        };
        Ok(Some(Self::Logged(LoggedOutcome {
            sequence: row.sequence,
            segment: at.segment(),
            offset: at.offset(),
            len: short,
            events,
            check,
        })))
    }
    /// A copy of the row: a held one's box, a spilled one's place.
    pub(super) fn copy(&self) -> Self {
        match self {
            Self::Held(row) => Self::Held(Box::new(**row)),
            Self::Logged(at) => Self::Logged(*at),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeEventKind {
    Monitor(NativeMonitorEvent),
    OwnerReleased,
    Validating,
    LocallyComplete,
    ValidationIncomplete,
    ValidationFailed,
    ValidationErrored,
    DependencyFailed,
    Created,
    ChildRegistered,
    Superseded,
    Cancelled,
    Posted,
    PostFailed,
    Received,
    ReceiptAdopted,
    Satisfied,
    TestamentGenerated,
    TestamentAcknowledged,
    ResponseObserved,
    Expired,
    Deadlocked,
    /// A legacy status fact carried by import; the value is the legacy
    /// sequence at which the legacy engine recorded it (23 §5.2).
    Imported(SessionSeq),
}
/// Ledger-local monitor facts carry their exact model cuts without duplicating
/// owner bindings. Each is one ordinary claim revision in the same journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeMonitorEvent {
    Registered {
        id: MonitorId,
        cut: focal_model::lifecycle::claim::ClaimCut,
    },
    Rebound {
        id: MonitorId,
        change: scope::Rebinding,
    },
    Released {
        id: MonitorId,
        cut: focal_model::lifecycle::claim::ClaimCut,
    },
    Cancelled {
        id: MonitorId,
        cancellation: scope::MonitorCancellation,
    },
}
impl NativeMonitorEvent {
    pub fn from_scope(event: scope::Event) -> Result<Self, ContractError> {
        match event {
            scope::Event::Registered { id, cut } => Ok(Self::Registered { id, cut }),
            scope::Event::Rebound { id, change } => Ok(Self::Rebound { id, change }),
            scope::Event::MonitorReleased { id, cut } => Ok(Self::Released { id, cut }),
            scope::Event::MonitorCancelled { id, cancellation } => {
                Ok(Self::Cancelled { id, cancellation })
            }
            _ => Err(ContractError::InvalidTransition),
        }
    }
    pub fn into_scope(self) -> scope::Event {
        match self {
            Self::Registered { id, cut } => scope::Event::Registered { id, cut },
            Self::Rebound { id, change } => scope::Event::Rebound { id, change },
            Self::Released { id, cut } => scope::Event::MonitorReleased { id, cut },
            Self::Cancelled { id, cancellation } => {
                scope::Event::MonitorCancelled { id, cancellation }
            }
        }
    }
    pub fn id(self) -> MonitorId {
        match self {
            Self::Registered { id, .. }
            | Self::Rebound { id, .. }
            | Self::Released { id, .. }
            | Self::Cancelled { id, .. } => id,
        }
    }
    pub fn cut(self) -> focal_model::lifecycle::claim::ClaimCut {
        match self {
            Self::Registered { cut, .. } | Self::Released { cut, .. } => cut,
            Self::Rebound { change, .. } => change.cut,
            Self::Cancelled { cancellation, .. } => cancellation.cut,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeEvent {
    pub invocation: NativeInvocation,
    pub sequence: SessionSeq,
    pub ordinal: u32,
    pub fact: NativeFact,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeFact {
    ResultTestament {
        claim: ClaimId,
        before: Option<Binding>,
        after: Binding,
        state: focal_model::lifecycle::audit::ResultTestamentState,
    },
    Missing {
        key: NativeResultKey,
    },
    Registrations {
        claim: Binding,
    },
    Delivery {
        key: NativeResultKey,
    },
    Work {
        claim: ClaimId,
        before: Option<Binding>,
        after: Binding,
        state: WorkArtifactState,
    },
    Diagnostic {
        claim: ClaimId,
        binding: Binding,
        reason: EvidenceFailure,
    },
    Response {
        claim: ClaimId,
        before: Option<Binding>,
        after: Binding,
        state: ResponseState,
    },
    Receipt {
        claim: Binding,
        fence: ReceiptFence,
        holder: ParticipantId,
    },
    ReceiptAdopted {
        claim: Binding,
        previous: ReceiptEntitlement,
        replacement: ReceiptEntitlement,
        cause: ContentHash,
    },
    Artifact {
        binding: Binding,
    },
    Accepted {
        key: NativeResultKey,
    },
    Claim(NativeClaimEvent),
    Definition {
        binding: Binding,
        claim: ClaimId,
        index: u32,
        intent: ContentHash,
    },
    Evaluation {
        kind: NativeEvaluationEventKind,
        key: EvaluationKey,
        before: Option<Binding>,
        after: Binding,
        state: validation::State,
        phase: validation::Phase,
        attempt: Option<validation::Attempt>,
        fence: Option<validation::AuthorityFence>,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeEvaluationEventKind {
    MissingTarget,
    Materialized,
    Begun,
    Reported,
    AuthorityFenced,
    Sealed,
}
impl NativeEvent {
    pub fn claim_event(self) -> Option<NativeClaimEvent> {
        match self.fact {
            NativeFact::Claim(event) => Some(event),
            _ => None,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeGraphCapture {
    /// Exclusive current-journal boundary of the actual graph snapshot. A
    /// batched reduction retains one shared boundary; recapture advances it.
    pub before_ordinal: u32,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeClaimEvent {
    pub kind: NativeEventKind,
    pub graph: Option<NativeGraphCapture>,
    pub owned_child: Option<Binding>,
    pub before: Option<Binding>,
    pub after: Binding,
    pub status: ClaimStatus,
}
impl NativeClaimEvent {
    pub(super) fn check_graph_capture(self, ordinal: u32) -> Result<(), ContractError> {
        let required = matches!(
            self.kind,
            NativeEventKind::DependencyFailed
                | NativeEventKind::Deadlocked
                | NativeEventKind::Satisfied
                | NativeEventKind::Expired
        );
        if self.graph.is_some() != required
            || self
                .graph
                .is_some_and(|capture| capture.before_ordinal > ordinal)
        {
            return Err(ContractError::InvalidCut);
        }
        Ok(())
    }
}

/// Lookup identity; the retained row additionally pins full target content,
/// revisions and receipt. A definition can have multiple independent targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct EvaluationKey {
    pub claim: ClaimId,
    pub validation: ValidationId,
    pub target: EvaluationTarget,
    pub generation: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum EvaluationTarget {
    Admission,
    Increment {
        artifact: ArtifactId,
    },
    Work {
        response: TestamentId,
        slot: u32,
        artifact: ArtifactId,
    },
    MissingSlot {
        response: TestamentId,
        slot: u32,
    },
    Delivery {
        response: TestamentId,
    },
}
impl EvaluationKey {
    fn of(claim: ClaimId, evaluation: &validation::EvaluationState) -> Self {
        Self {
            claim,
            validation: ValidationId(evaluation.binding().object.0),
            target: EvaluationTarget::of(evaluation.target()),
            generation: evaluation.generation(),
        }
    }
}
impl EvaluationTarget {
    /// The lookup identity of a pinned evaluation target.
    pub fn of(target: validation::Target) -> Self {
        use validation::Target;
        match target {
            Target::Admission { .. } => EvaluationTarget::Admission,
            Target::Increment { artifact, .. } => EvaluationTarget::Increment {
                artifact: ArtifactId(artifact.object.0),
            },
            Target::Artifact {
                response,
                slot,
                artifact,
            } => EvaluationTarget::Work {
                response: TestamentId(response.object.0),
                slot,
                artifact: ArtifactId(artifact.object.0),
            },
            Target::MissingSlot { response, slot } => EvaluationTarget::MissingSlot {
                response: TestamentId(response.object.0),
                slot,
            },
            Target::Delivery { response } => EvaluationTarget::Delivery {
                response: TestamentId(response.object.0),
            },
        }
    }
}

/// Immutable allocation of an execution receipt. This row survives later
/// receipt control so an old identity can never be recycled for another claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeReceipt {
    pub claim: ClaimId,
    pub fence: ReceiptFence,
    pub holder: ParticipantId,
    pub acquired: SessionSeq,
}

/// Exact owner-resolved responsibility and work cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct NativeCycleKey {
    pub claim: ClaimId,
    pub receipt: ReceiptId,
    pub epoch: u64,
    pub cycle: u32,
}
impl NativeCycleKey {
    pub(super) fn of(parent: &evidence::Parent) -> Self {
        Self {
            claim: parent.claim,
            receipt: parent.receipt.receipt,
            epoch: parent.receipt.epoch,
            cycle: parent.next_cycle,
        }
    }
}
/// Owner-maintained membership; every entry is retained under this exact cycle.
/// Appending evidence edits one head and row, without copying a growing list.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct NativeCycle {
    pub work_head: Option<ArtifactId>,
    pub work_count: usize,
    pub diagnostic_head: Option<ArtifactId>,
    pub diagnostic_count: usize,
    pub response: Option<TestamentId>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct RetiredCycleHead {
    pub head: Option<NativeCycleKey>,
    pub count: usize,
    pub work_count: usize,
}
#[derive(Debug, Clone, Copy)]
pub(super) struct RetiredCycle {
    pub holder: ParticipantId,
    pub next: Option<NativeCycleKey>,
}

/// Address of an immutable accepted attempt, separate from its mutable evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct NativeResultKey {
    pub evaluation: EvaluationKey,
    pub revision: focal_model::ObjectRevision,
}
impl NativeResultKey {
    pub fn of(result: validation::AcceptedResult) -> Self {
        Self {
            evaluation: EvaluationKey {
                claim: result.claim(),
                validation: result.validation(),
                target: EvaluationTarget::of(result.target()),
                generation: result.generation(),
            },
            revision: result.binding().revision,
        }
    }
}

/// Ordered by the storage layout ([`layout`]): affinity, family, fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Key {
    IncomingHead(ClaimId),
    IncomingLink(ClaimId, ClaimId),
    Monitor(focal_model::MonitorId),
    MonitorHead(ClaimId),
    MonitorLink(ClaimId, focal_model::MonitorId),
    MissingResult(NativeResultKey),
    Meta,
    Claim(ClaimId),
    Definition(ValidationId),
    Evaluation(EvaluationKey),
    Artifact(ArtifactId),
    ArtifactIdentity(ContentHash),
    Accepted(NativeResultKey),
    DeliveryResult(NativeResultKey),
    Receipt(ReceiptId),
    Cycle(NativeCycleKey),
    RetiredCycleHead(ClaimId),
    RetiredCycle(NativeCycleKey),
    Work(ArtifactId),
    WorkSlot(NativeCycleKey, u32),
    Diagnostic(ArtifactId),
    Response(TestamentId),
    ResultTestament(TestamentId),
    ClaimResultTestament(ClaimId),
    Outcome(NativeInvocation),
    Event(SessionSeq, u32),
    ClaimContent(ClaimId),
    ClaimIdentity(u16, ContentHash),
    DefinitionIdentity(u16, ContentHash),
    CreationResult(NativeInvocation),
    LegacyTestament(TestamentId),
    LegacyEvidenceSet(focal_model::EvidenceSetId),
    LegacyRun(ValidationId, u32),
    LegacyDefinition(ValidationId),
    /// Secondary index families (doc 22 §7). Unit rows derived from exactly
    /// one primary row; replay and recovery check them against it.
    ByIssuer(ParticipantId, ClaimId),
    BySubject(ParticipantId, ClaimId),
    ByStatus(u16, ClaimId),
    ByAction(u16, ClaimId),
    ByScope(u16, ContentHash, ClaimId),
    ByRelation(u16, ClaimId, ClaimId),
    ByProducer(ParticipantId, ArtifactId),
    ByArtifactKind(ContentHash, ArtifactId),
    BySchema(ContentHash, ArtifactId),
    ArtifactInput(focal_model::ObjectId, ArtifactId),
    ByEvaluator(ParticipantId, ValidationId),
    ByVerdict(u16, NativeResultKey),
    ByCreated(u16, SessionSeq, focal_model::ObjectId),
    /// A trusted timer that is due at the logical time and has not been
    /// consumed: the claim's deadline, an evaluation's declaration deadline
    /// or a monitor's deadline (22 §7).
    DueTimer(u64, TimerTarget),
    /// The identity index of a primary object family (22 §7): one unit row
    /// per claim, artifact or declaration under its family's bucket, so a
    /// listing in identity order is one contiguous scan although each
    /// object's own rows sit under the object (25 §3).
    ByObject(u16, focal_model::ObjectId),
    /// The typed continuation of a claim retired to the archive (26 §4):
    /// written where the claim's rows were, so a reference resolves to the
    /// bundle that holds them rather than to nothing.
    Retired(ClaimId),
    /// A principal's request generations (F12): under the control affinity.
    Epochs(ParticipantId),
    /// A seal of closed outcomes, by ordinal (F12): under the control
    /// affinity; a fold's row is keyed by the last ordinal it covers.
    Seal(u64),
    End,
}

/// The primary row a due timer belongs to. Its deadline identity (timer and
/// generation) is read from that row when the timer is delivered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TimerTarget {
    Claim(ClaimId),
    Evaluation(EvaluationKey),
    Monitor(ClaimId, MonitorId),
}
impl TimerTarget {
    /// The invocation delivering this timer with `deadline`.
    pub fn invocation(self, deadline: Deadline) -> NativeInvocation {
        match self {
            Self::Claim(claim) => NativeInvocation::ClaimDeadline(NativeClaimDeadlineKey {
                claim,
                timer: deadline.timer,
                generation: deadline.generation,
            }),
            Self::Evaluation(evaluation) => {
                NativeInvocation::EvaluationDeadline(NativeDeadlineKey {
                    evaluation,
                    timer: deadline.timer,
                    generation: deadline.generation,
                })
            }
            Self::Monitor(claim, monitor) => {
                NativeInvocation::MonitorDeadline(NativeMonitorDeadlineKey {
                    claim,
                    monitor,
                    timer: deadline.timer,
                    generation: deadline.generation,
                })
            }
        }
    }
}

/// The page class of a row: the session's counters apart from every other row. The counters are
/// rewritten by every mutation; sharing class 0 with the events put them on the oldest events'
/// page, so every mutation deep-copied that page's rows (the 2026-09-29 allocation audit's R4:
/// about a hundred 504-byte event copies a claim). On a page of their own a rewrite copies one row.
///
/// Every other family shares one class (doc 31 M3). Classes were also kept for the immutable
/// payloads (definitions, artifacts, content, creation results) so a lifecycle rewrite would not
/// copy them, but a claim's own rows sit together in the key order (its state beside its
/// content, an outcome beside its creation result), so the classes alternated row by row and
/// cut a page at nearly every row: 2.3 rows a page at the node's 4 KiB leaves, a fixed charge
/// each, 400 bytes a claim. A leaf is bounded by its bytes, heap included, so what a copy-on-write
/// copies is a page's worth whichever rows share it, and a page is copied only where a capture or
/// a lease still reads it: publication edits it in place.
fn page_partition(key: &layout::PackedKey) -> u64 {
    // By the key's family (its bytes after the affinity), as the store places every key.
    match key.family() {
        6 => 1,
        _ => 0,
    }
}
#[derive(Debug, Default, Clone, Copy)]
struct Meta {
    claims: usize,
    outcomes: usize,
    events: usize,
    definitions: usize,
    evaluations: usize,
    artifacts: usize,
    results: usize,
    receipts: usize,
    responses: usize,
    result_testaments: usize,
    monitors: usize,
    monitor_links: usize,
    creation_results: usize,
    legacy: usize,
    /// Outcomes that left the core into a seal or a retirement bundle
    /// (F12): resident outcomes are `outcomes - sealed`.
    sealed: usize,
    /// Event rows of the outcomes that left: what the resident events and
    /// the retired continuations reconcile against.
    sealed_events: usize,
    /// Seals applied: the next seal's ordinal is one more.
    seals: usize,
    /// Principals with a request generation window.
    principals: usize,
    logical_time: u64,
}
/// A resumable position in the committed rows, opaque to callers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeRowCursor(Key);
/// One content object the committed rows name (26 §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentRoot {
    /// An artifact's payload held as a content object.
    Artifact {
        artifact: ArtifactId,
        pointer: focal_model::lifecycle::artifact_descriptor::ContentPointer,
    },
    /// An artifact's payload held inline: the object every replica sealed
    /// at admission under the canonical chunking, so its root is the same
    /// on every node and the record names it.
    Inline {
        artifact: ArtifactId,
        pointer: focal_model::lifecycle::artifact_descriptor::ContentPointer,
    },
    /// A retired family's archive bundle, by the retired claim, content
    /// root and length.
    Bundle {
        claim: ClaimId,
        root: ContentHash,
        bytes: u64,
    },
    /// A seal's bundle (F12), by the seal's ordinal, content root and
    /// length: the proof of the outcomes that left the core.
    Seal {
        ordinal: u64,
        root: ContentHash,
        bytes: u64,
    },
    /// A family retired to every replica's cold stream (doc 31 M2, A2), by the retired claim:
    /// its frame's digest and length and the retirement's sequence, where the stream's marks find
    /// it. Not a content object: what custody must keep of it is what its frame names.
    Cold {
        claim: ClaimId,
        digest: ContentHash,
        bytes: u64,
        retired_at: SessionSeq,
    },
}
impl ContentRoot {
    /// The object's content root: a cold family's frame digest, which names no content object.
    pub fn root(&self) -> ContentHash {
        match self {
            Self::Artifact { pointer, .. } | Self::Inline { pointer, .. } => pointer.root,
            Self::Bundle { root, .. } | Self::Seal { root, .. } => *root,
            Self::Cold { digest, .. } => *digest,
        }
    }
}
/// One page of [`Core::native_content_roots`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentRootsPage {
    pub roots: Vec<ContentRoot>,
    pub next: Option<NativeRowCursor>,
    pub visited: usize,
}
/// The continuation left where a retired claim's rows were (26 §4): the
/// archive bundle that holds them, the retention prefix the retirement
/// named, the claim's final binding and status, and the native sequence
/// the retirement was published at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetiredClaim {
    /// The content root of the archive bundle, an object of the ledger's
    /// tenant domain under the checkpoint class.
    pub bundle: ContentHash,
    /// The bundle's length: with the root, what names the object.
    pub bytes: u64,
    pub through: SessionSeq,
    pub binding: Binding,
    pub status: ClaimStatus,
    pub retired_at: SessionSeq,
    /// Event rows that left the core with this member; outcome rows keep
    /// counting them, so validation reconciles the two.
    pub events: u32,
}
/// A row of the native store. Every entry is as wide as its widest row, and a claim
/// writes mostly index rows that carry nothing (`Index`) or a pointer (`Owned*`): a
/// payload wider than a pointer is kept behind one, charged to its entry's heap
/// (`wide_heap`), so that the commonest rows are not 160 bytes each (2.3 KB a claim).
#[derive(Debug)]
enum Row {
    IncomingHead(Box<incoming_graph::IncomingHead>),
    IncomingLink(incoming_graph::IncomingLink),
    Monitor(Box<monitor_index::MonitorAllocation>),
    MonitorHead(Box<monitor_index::MonitorHead>),
    MonitorLink(Option<Box<monitor_index::MonitorLink>>),
    MissingResult(OwnedMissingResult),
    Meta(Box<Meta>),
    Claim(OwnedClaim),
    Definition(OwnedDeclaration),
    Evaluation(OwnedEvaluation),
    Artifact(OwnedArtifact),
    ArtifactIdentity(ArtifactId),
    Accepted(OwnedAccepted),
    DeliveryResult(OwnedDeliveryResult),
    Receipt(Box<NativeReceipt>),
    Cycle(Box<NativeCycle>),
    RetiredCycleHead(Box<RetiredCycleHead>),
    RetiredCycle(Box<RetiredCycle>),
    Work(OwnedWork),
    WorkSlot(ArtifactId),
    Diagnostic(OwnedDiagnostic),
    Response(OwnedResponse),
    ResultTestament(OwnedResultTestament),
    ClaimResultTestament(TestamentId),
    Outcome(OwnedOutcome),
    Event(OwnedEvent),
    ClaimContent(OwnedClaimContent),
    ClaimIdentity(ClaimId),
    DefinitionIdentity(ValidationId),
    CreationResult(OwnedCreationResult),
    LegacyTestament(OwnedLegacy),
    LegacyEvidenceSet(OwnedLegacy),
    LegacyRun(OwnedLegacy),
    LegacyDefinition(OwnedLegacy),
    /// The unit value of every secondary index row.
    Index,
    Retired(Box<RetiredClaim>),
    Epochs(Box<EpochWindow>),
    Seal(Box<SealRow>),
}

/// The heap a row's boxed payload of type `T` takes: its bytes and the allocator's own.
const fn wide_heap<T>() -> usize {
    match size_of::<T>().checked_add(focal_memory::ALLOCATOR_OVERHEAD) {
        Some(bytes) => bytes,
        None => usize::MAX,
    }
}

/// The most heap any one row's box takes: the widest boxed payload's.
fn wide_heap_max() -> usize {
    [
        wide_heap::<incoming_graph::IncomingHead>(),
        wide_heap::<monitor_index::MonitorAllocation>(),
        wide_heap::<monitor_index::MonitorHead>(),
        wide_heap::<monitor_index::MonitorLink>(),
        wide_heap::<Meta>(),
        wide_heap::<NativeReceipt>(),
        wide_heap::<NativeCycle>(),
        wide_heap::<RetiredCycleHead>(),
        wide_heap::<RetiredCycle>(),
        wide_heap::<OutcomeRow>(),
        wide_heap::<RetiredClaim>(),
        wide_heap::<EpochWindow>(),
        wide_heap::<SealRow>(),
    ]
    .into_iter()
    .max()
    .unwrap_or(0)
}

impl Key {
    /// The most heap the row under this key may take in a box (`Row`): what a
    /// quote made before the row is built allows for it.
    fn boxed_heap(&self) -> usize {
        match self {
            Key::IncomingHead(..) => wide_heap::<incoming_graph::IncomingHead>(),
            Key::Monitor(..) => wide_heap::<monitor_index::MonitorAllocation>(),
            Key::MonitorHead(..) => wide_heap::<monitor_index::MonitorHead>(),
            Key::MonitorLink(..) => wide_heap::<monitor_index::MonitorLink>(),
            Key::Meta => wide_heap::<Meta>(),
            Key::Receipt(..) => wide_heap::<NativeReceipt>(),
            Key::Cycle(..) => wide_heap::<NativeCycle>(),
            Key::RetiredCycleHead(..) => wide_heap::<RetiredCycleHead>(),
            Key::RetiredCycle(..) => wide_heap::<RetiredCycle>(),
            Key::Outcome(..) => wide_heap::<OutcomeRow>(),
            Key::Retired(..) => wide_heap::<RetiredClaim>(),
            Key::Epochs(..) => wide_heap::<EpochWindow>(),
            Key::Seal(..) => wide_heap::<SealRow>(),
            _ => 0,
        }
    }
}

impl Row {
    /// The heap this row's boxed payload takes, if it has one.
    fn boxed_heap(&self) -> usize {
        match self {
            Row::IncomingHead(_) => wide_heap::<incoming_graph::IncomingHead>(),
            Row::Monitor(_) => wide_heap::<monitor_index::MonitorAllocation>(),
            Row::MonitorHead(_) => wide_heap::<monitor_index::MonitorHead>(),
            Row::MonitorLink(Some(_)) => wide_heap::<monitor_index::MonitorLink>(),
            Row::Meta(_) => wide_heap::<Meta>(),
            Row::Receipt(_) => wide_heap::<NativeReceipt>(),
            Row::Cycle(_) => wide_heap::<NativeCycle>(),
            Row::RetiredCycleHead(_) => wide_heap::<RetiredCycleHead>(),
            Row::RetiredCycle(_) => wide_heap::<RetiredCycle>(),
            Row::Outcome(OwnedOutcome::Held(_)) => wide_heap::<OutcomeRow>(),
            Row::Retired(_) => wide_heap::<RetiredClaim>(),
            Row::Epochs(_) => wide_heap::<EpochWindow>(),
            Row::Seal(_) => wide_heap::<SealRow>(),
            _ => 0,
        }
    }
}

/// A store entry: `heap` is what the row's own owned bytes take; a boxed payload's
/// allocation is added here, once, for every row however it was made.
fn entry(key: Key, row: Row, heap: usize) -> focal_memory::Entry<Key, Row> {
    let boxed = row.boxed_heap();
    focal_memory::Entry::new(key, row, heap.saturating_add(boxed))
}

/// All allocated candidate rows, outcomes and events have one immutable root.
/// Dropping a candidate releases its page permits without touching live state.
pub struct NativePrepared {
    fragments: ranges::Fragments,
    outcome: NativeOutcome,
    writes: mutation::WriteSet,
    /// What the rows this mutation puts take in a checkpoint: its whole addition, before what
    /// it replaces or deletes is taken away at publication.
    encoded_added: usize,
    /// What the committed rows this mutation replaces or deletes take in a checkpoint, as its
    /// build took them out of their pages: what its publication takes away. Built on the root
    /// its predecessors leave, which is the root it publishes onto.
    encoded_removed: usize,
    /// What its changes may take in an owner's pending index
    /// ([`pending_index::entry_charge`]), funded with the rest of it.
    _index: Option<focal_memory::Allocation>,
    /// What its events take in the event index (doc 31 M2,
    /// [`event_index::entry_charge`]), funded with the rest of it and handed to
    /// the index as it publishes.
    events: Option<focal_memory::Allocation>,
    /// A retirement's families and their bundles (doc 31 M2): what its record
    /// carries in place of its rows, every replica deriving the rest.
    retire: Option<retirement_batch::RetireBody>,
}
impl std::fmt::Debug for NativePrepared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativePrepared")
            .field("base", &self.fragments.base_prefix())
            .field("outcome", &self.outcome)
            .finish_non_exhaustive()
    }
}
impl NativePrepared {
    /// Number of actual storage writes, including metadata, indices and history.
    pub fn mutation_count(&self) -> usize {
        self.writes.len()
    }
    /// The positions, in layout order, of the group members this mutation
    /// writes (25 §6): what a movement fence checks before admission.
    pub fn touched_members(&self) -> impl Iterator<Item = usize> + '_ {
        self.fragments.touched_members()
    }
    pub fn mutation_heap_bytes(&self) -> usize {
        self.writes.heap_bytes()
    }
    /// What it holds of the `Pending` kind besides its write set: its batch,
    /// its merge staging and its place in an owner's pending index.
    #[cfg(test)]
    pub(crate) fn held_pending_bytes(&self) -> usize {
        self.fragments.pending_bytes()
            + self
                ._index
                .as_ref()
                .map_or(0, focal_memory::Allocation::bytes)
    }
    pub fn content_profile(&self) -> NativeContentProfile {
        self.writes.profile()
    }
    pub fn outcome(&self) -> NativeOutcome {
        self.outcome
    }
    pub fn written_claim(&self, id: ClaimId) -> Option<&ClaimState> {
        as_claim(self.fragments.get(&Key::Claim(id)))
    }
    pub fn written_receipt(&self, id: ReceiptId) -> Option<NativeReceipt> {
        as_receipt(self.fragments.get(&Key::Receipt(id)))
    }
    pub fn written_artifact(&self, id: ArtifactId) -> Option<&NativeArtifact> {
        as_artifact(self.fragments.get(&Key::Artifact(id)))
    }
    pub fn written_result(&self, key: NativeResultKey) -> Option<&NativeAccepted> {
        as_result(self.fragments.get(&Key::Accepted(key)))
    }
    /// The outcome this candidate records for `key`, read back through `payloads` (its core's,
    /// [`Core::native_payloads`]) where it is spilled.
    pub fn written_recorded(
        &self,
        key: impl Into<NativeInvocation>,
        payloads: Option<&dyn payloads::PayloadRead>,
    ) -> Result<Option<NativeOutcome>, NativeError> {
        let invocation = key.into();
        as_outcome(
            self.fragments.get(&Key::Outcome(invocation)),
            self.outcome.ledger,
            invocation,
            payloads,
        )
    }
    pub fn written_definition(&self, id: ValidationId) -> Option<&validation::Declaration> {
        as_definition(self.fragments.get(&Key::Definition(id)))
    }
    pub fn written_evaluation(&self, key: EvaluationKey) -> Option<&validation::EvaluationState> {
        as_evaluation(self.fragments.get(&Key::Evaluation(key)))
    }
}
#[derive(Debug)]
#[allow(
    clippy::large_enum_variant,
    reason = "moved once, from a preparation into its owner's chain or a publication"
)]
pub enum NativePreparation {
    Prepared(NativePrepared),
    /// A pending match must await the original candidate's durability barrier.
    Existing {
        outcome: NativeOutcome,
        committed: bool,
    },
}
#[derive(Debug, thiserror::Error)]
pub enum NativeError {
    #[error("native lifecycle admission: {0}")]
    Contract(#[from] ContractError),
    #[error("native owner memory: {0}")]
    Memory(#[from] MemoryError),
    #[error("native evidence: {0}")]
    Evidence(#[from] focal_evidence::NativeEvidenceError),
    #[error("request key was already used for a different native intent")]
    RequestConflict,
    #[error("native owner bound exceeded: {0}")]
    Capacity(&'static str),
    /// A spilled row's bytes could not be read back from the replica's payload log (doc 31 M3).
    #[error("native payload log: {0}")]
    Payload(#[from] payloads::PayloadError),
}
/// A refused publication. Refused before any row moved, the candidate is
/// returned whole; failed past that point (an invariant broken, its store
/// poisoned), it is gone and the owner must be reconstructed.
#[derive(Debug)]
pub struct NativePublishError {
    pub error: MemoryError,
    pub prepared: Option<NativePrepared>,
}
impl std::fmt::Display for NativePublishError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}
impl std::error::Error for NativePublishError {}

/// A native core's committed rows frozen at one prefix ([`Core::freeze_native`]):
/// read-only and sendable, so a checkpoint encodes from it off the owner's
/// thread. Its pages stay charged until it drops; its holder bounds its life.
pub struct NativeFrozen {
    ledger: LedgerId,
    profile: NativeContentProfile,
    rows: ranges::FrozenRanges,
    /// A reader of the payload log the frozen rows' spilled bytes are in.
    payloads: Option<Box<dyn payloads::PayloadRead + Send>>,
}
impl std::fmt::Debug for NativeFrozen {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeFrozen")
            .field("ledger", &self.ledger)
            .field("prefix", &self.rows.prefix())
            .field("entries", &self.rows.len())
            .finish_non_exhaustive()
    }
}
impl NativeFrozen {
    pub fn native_sequence(&self) -> SessionSeq {
        SessionSeq(self.rows.prefix())
    }
    /// Where the frozen rows' spilled bytes are read from.
    pub(crate) fn payloads(&self) -> Option<&dyn payloads::PayloadRead> {
        self.payloads
            .as_deref()
            .map(|reader| -> &dyn payloads::PayloadRead { reader })
    }
}

/// Expiring capability to a fixed native prefix. Borrowed rows cannot escape a
/// projection call. No Clone implementation is needed on native claim rows.
#[derive(Debug)]
pub struct NativeRead {
    lease: ranges::RangeLeases,
    /// The store's ledger, which an outcome row does not repeat.
    ledger: LedgerId,
    /// A reader of the payload log the leased rows' spilled bytes are in, with handles of its
    /// own: a sweep after the lease was taken never breaks its reads (doc 31 M3).
    payloads: Option<Box<dyn payloads::PayloadRead + Send>>,
    /// The bounds a spilled claim read back through the lease is built under, and the budget
    /// what it reads back is charged to while its projection runs (doc 31 M3).
    limits: NativeLimits,
    budget: MemoryBudget,
}
impl NativeRead {
    fn payload_source(&self) -> Option<&dyn payloads::PayloadRead> {
        self.payloads
            .as_deref()
            .map(|read| -> &dyn payloads::PayloadRead { read })
    }
    /// The row the lease holds at `key`, projected; a spilled claim or definition read back for
    /// the projection (doc 31 M3): the leased prefix held while it is decoded and projected, what
    /// it names read there, and everything read back let go with the call.
    fn project_row<T>(
        &self,
        key: Key,
        now: u64,
        project: impl FnOnce(&Row) -> T,
    ) -> Result<Option<T>, NativeError> {
        let mut project = Some(project);
        let found = self
            .lease
            .project_next(&key, false, &Key::End, now, |entry| {
                if entry.key != key {
                    return Ok(None);
                }
                let spilled = match entry.value {
                    Row::Claim(claim) => claim.logged(),
                    Row::Definition(definition) => definition.logged(),
                    _ => None,
                };
                match spilled {
                    Some(at) => Err(at),
                    None => Ok(project.take().map(|project| project(entry.value))),
                }
            })?;
        let (at, project) = match (found, project) {
            (Some(Err(at)), Some(project)) => (at, project),
            (Some(Ok(projected)), _) => return Ok(projected),
            _ => return Ok(None),
        };
        let rows = self.lease.hold(now, &self.budget)?;
        let reads = read_back::ReadBack::new(self.limits);
        let names = LeaseRows {
            rows: &rows,
            reads: &reads,
            payloads: self.payload_source(),
            budget: &self.budget,
        };
        let row = match key {
            Key::Claim(id) => {
                let fund = |bytes| reads.fund(&self.budget, bytes);
                let (claim, charge) = with_spilled(self.payload_source(), at, |bytes| {
                    record_codec::decode_claim(id, bytes, self.ledger, &names, self.limits, &fund)
                })?;
                reads.hold(&self.budget, at, Row::Claim(claim), charge)?
            }
            _ => read_back_definition(&reads, self.payload_source(), &self.budget, at)?,
        };
        reads.failure()?;
        Ok(Some(project(row)))
    }
    pub fn sequence(&self) -> SessionSeq {
        SessionSeq(self.lease.prefix())
    }
    pub fn with_claim<T>(
        &self,
        id: ClaimId,
        now: u64,
        project: impl FnOnce(&ClaimState) -> T,
    ) -> Result<Option<T>, MemoryError> {
        self.project_row(Key::Claim(id), now, |row| as_claim(Some(row)).map(project))
            .map(Option::flatten)
            .map_err(lease_read_back)
    }
    pub fn recorded(
        &self,
        request: impl Into<NativeInvocation>,
        now: u64,
    ) -> Result<Option<NativeOutcome>, NativeError> {
        let invocation = request.into();
        let key = Key::Outcome(invocation);
        let payloads = self
            .payloads
            .as_deref()
            .map(|read| -> &dyn payloads::PayloadRead { read });
        let projected = self
            .lease
            .project_next(&key, false, &Key::End, now, |entry| {
                if entry.key == key {
                    as_outcome(Some(entry.value), self.ledger, invocation, payloads)
                } else {
                    Ok(None)
                }
            })?;
        projected.transpose().map(Option::flatten)
    }
    pub fn with_definition<T>(
        &self,
        id: ValidationId,
        now: u64,
        project: impl FnOnce(&validation::Declaration) -> T,
    ) -> Result<Option<T>, MemoryError> {
        self.project_row(Key::Definition(id), now, |row| {
            as_definition(Some(row)).map(project)
        })
        .map(Option::flatten)
        .map_err(lease_read_back)
    }
    pub fn with_evaluation<T>(
        &self,
        id: EvaluationKey,
        now: u64,
        project: impl FnOnce(&validation::EvaluationState) -> T,
    ) -> Result<Option<T>, MemoryError> {
        let key = Key::Evaluation(id);
        self.lease
            .project_next(&key, false, &Key::End, now, |entry| {
                if entry.key == key {
                    as_evaluation(Some(entry.value)).map(project)
                } else {
                    None
                }
            })
            .map(Option::flatten)
    }
}

/// Shared local and recovered owner configuration checks.
fn checked_native_limits(
    ledger: LedgerId,
    mut limits: NativeLimits,
) -> Result<NativeLimits, NativeError> {
    if ledger.tenant.is_zero() || ledger.session.is_zero() {
        return Err(ContractError::WrongLedger.into());
    }
    if limits.pending == 0
        || limits.plan_nodes == 0
        || limits.plan_edges == 0
        || limits.preparation_bytes == 0
        || limits.claims == 0
        || limits.outcomes == 0
        || limits.principals == 0
        || limits.seals < 2
        || limits.events == 0
        || limits.definitions == 0
        || limits.evaluations == 0
        || limits.evaluations_per_claim == 0
        || limits.artifacts == 0
        || limits.results == 0
        || limits.receipts == 0
        || limits.responses == 0
        || limits.work_artifacts_per_cycle == 0
        || limits.diagnostics_per_cycle == 0
        || limits.artifact_inputs == 0
        || limits.response_summary_bytes == 0
        || limits.max_ranges == 0
        || limits.max_ranges > ranges::MAX_LAYOUT_MEMBERS
        || limits.range.max_batch_entries < 4
    {
        return Err(MemoryError::InvalidConfiguration("native limits must be nonzero").into());
    }
    // Native mutations already bound nested row heaps through Scratch;
    // moved claim/event singleton containers have their separate precharge.
    // Preserve tighter node-derived limits and isolate larger admitted rows
    // so an unrelated small write cannot copy their payloads as neighbors.
    let entry_ceiling = prepare::add(
        prepare::add(
            limits.preparation_bytes,
            prepare::add(
                OwnedClaim::container_charge(),
                OwnedEvent::container_charge(),
            )?,
        )?,
        size_of::<focal_memory::Entry<Key, Row>>(),
    )?;
    // Without a tighter bound from the node, a leaf of rows with heap costs no more to rebuild than a full
    // leaf of inline rows.
    let inline_leaf = limits.range.inline_leaf_bytes::<Key, Row>()?;
    limits.range.page_bytes = limits.range.page_bytes.min(inline_leaf);
    limits.range.max_entry_bytes = limits.range.max_entry_bytes.min(entry_ceiling);
    Ok(limits)
}

impl Core<NativeState> {
    pub fn new_native(
        ledger: LedgerId,
        range: RangeId,
        limits: NativeLimits,
        budget: MemoryBudget,
    ) -> Result<Self, NativeError> {
        Self::new_native_profile(
            ledger,
            range,
            limits,
            budget,
            NativeContentProfile::ProjectionOnly,
        )
    }
    /// Start an empty native owner that requires complete authored content.
    /// This never promotes a projection-only root or activates a durable codec.
    pub fn new_native_authored(
        ledger: LedgerId,
        range: RangeId,
        limits: NativeLimits,
        budget: MemoryBudget,
    ) -> Result<Self, NativeError> {
        Self::new_native_profile(
            ledger,
            range,
            limits,
            budget,
            NativeContentProfile::AuthoredV1,
        )
    }
    fn new_native_profile(
        ledger: LedgerId,
        range: RangeId,
        mut limits: NativeLimits,
        budget: MemoryBudget,
        profile: NativeContentProfile,
    ) -> Result<Self, NativeError> {
        limits = checked_native_limits(ledger, limits)?;
        let rows = ranges::NativeRanges::new(range, limits.range, &budget, page_partition)?;
        let events = event_index::EventIndex::empty(&budget)?;
        let payloads = test_payloads();
        let mut reads = read_back::ReadBack::new(limits);
        if payloads.is_some() {
            reads.hold_floor(&budget);
        }
        Ok(Self {
            state: NativeState {
                ledger,
                profile,
                rows,
                budget,
                encoded_rows: 0,
                events,
                payloads,
                reads,
                young: young::Young::none(),
            },
            limits,
        })
    }
    /// Spill the rows this core builds from now on to `log` (doc 31 M3), where the replica keeps
    /// its payloads. Rows already held keep their bytes.
    /// As [`Self::attach_payload_log`], the young generation `bound` rows (none at zero: each
    /// spillable row spills where it is published).
    #[cfg(test)]
    pub(crate) fn attach_payload_log_with_young(
        &mut self,
        log: Box<dyn payloads::PayloadLog>,
        bound: usize,
    ) {
        self.attach_payload_log(log);
        self.state.young = young::Young::bounded(&self.state.budget, bound).unwrap();
    }
    pub fn attach_payload_log(&mut self, log: Box<dyn payloads::PayloadLog>) {
        self.state.reads.release();
        self.state.reads.hold_floor(&self.state.budget);
        // A generation the budget refuses leaves none: each row spills where it is published.
        self.state.young =
            young::Young::for_budget(&self.state.budget).unwrap_or_else(|_| young::Young::none());
        self.state.payloads = Some(log);
    }
    /// The payload log this core spills to, given back (for the core that replaces this one).
    pub fn detach_payload_log(&mut self) -> Option<Box<dyn payloads::PayloadLog>> {
        self.state.reads.release();
        self.state.payloads.take()
    }
    /// Lets go of the spilled rows reads have taken back since the core last changed (doc 31
    /// M3), and what they were charged: what a session does between its operations when it is
    /// not publishing, so reads alone never keep them.
    pub fn release_native_reads(&mut self) {
        self.state.reads.release();
    }
    /// The first spilled row reads failed to take back since this was last asked (doc 31 M3),
    /// which the operation that read fails with: a read that met one was lent no row.
    pub fn native_read_failure(&self) -> Result<(), NativeError> {
        self.state.reads.failure()
    }
    /// Whether the session is short of memory (doc 31 M3): its ordinary headroom below an eighth
    /// of its allowance, where its young generation spills its oldest rows and its finished
    /// families retire without waiting out their grace ([`Self::retirement_due`]).
    pub fn native_memory_short(&self) -> bool {
        young::short(&self.state.budget)
    }
    /// Whether `family`, which may retire, is due to at logical time `now` under a grace of
    /// `grace_ms` (26 §4): its last event settled that long ago or longer, or the session is short
    /// of memory, which waives the grace, since a finished family is the first memory a session
    /// gets back (doc 31 §4.8). `None` where the event or the outcome that settled it is missing.
    /// The grace is the offering authority's alone: a replica checks what a family holds, never
    /// its age.
    pub fn retirement_due(
        &self,
        family: &retirement::RetirementFamily,
        now: u64,
        grace_ms: u64,
    ) -> Result<Option<bool>, NativeError> {
        let Some(event) = self.native_event(family.through, 0) else {
            return Ok(None);
        };
        let Some(outcome) = self.native_outcome(event.invocation)? else {
            return Ok(None);
        };
        Ok(Some(
            self.native_memory_short() || now.saturating_sub(outcome.logical_time) >= grace_ms,
        ))
    }
    /// The young generation (doc 31 M3): the keys it tracks, the spills it put off and kept to
    /// try again, and the keys it could not keep, since it was made.
    pub fn native_young(&self) -> (usize, u64, u64) {
        let (put_off, untracked) = self.state.young.counts();
        (self.state.young.len(), put_off, untracked)
    }
    /// Rows reads have taken back since the core last changed, and the bytes they hold.
    pub fn native_read_backs(&self) -> (usize, usize) {
        (self.state.reads.rows(), self.state.reads.bytes())
    }
    /// The payload log this core spills to, lent (to a restore that spills to it as well).
    pub fn payload_log(&self) -> Option<&dyn payloads::PayloadLog> {
        self.state.payloads.as_deref()
    }
    /// Where this core's spilled rows' bytes are read from: what a candidate's own reads take
    /// ([`NativePrepared::written_claim_content`]).
    pub fn native_payloads(&self) -> Option<&dyn payloads::PayloadRead> {
        self.state.payloads()
    }
    /// The segments of the payload log a committed row references, added to `live`.
    pub(crate) fn live_payload_segments(&self, live: &mut std::collections::BTreeSet<u32>) {
        payloads::segments_of(
            self.state
                .rows
                .entries_from(&Key::Event(SessionSeq(0), 0), false)
                .take_while(|entry| matches!(entry.key, Key::Event(..)))
                .map(|entry| entry.value),
            live,
        );
    }
    /// Let go of the payload log's sealed segments no committed row references (doc 31 M3):
    /// for a core with no candidates pending. How many it let go.
    pub fn sweep_payloads(&self) -> usize {
        let Some(log) = self.state.payloads.as_deref() else {
            return 0;
        };
        let mut live = std::collections::BTreeSet::new();
        self.live_payload_segments(&mut live);
        log.sweep(&live)
    }
    pub fn native_sequence(&self) -> SessionSeq {
        SessionSeq(self.state.rows.prefix())
    }
    /// The committed rows frozen at this prefix, for a checkpoint to encode
    /// on another thread while this core goes on (`record_codec::checkpoint`).
    pub fn freeze_native(&self) -> Result<NativeFrozen, MemoryError> {
        let payloads = match self.state.payloads.as_deref() {
            Some(log) => Some(log.reader().map_err(|_| MemoryError::AllocationFailed)?),
            None => None,
        };
        Ok(NativeFrozen {
            ledger: self.state.ledger,
            profile: self.state.profile,
            rows: self.state.rows.freeze(&self.state.budget)?,
            payloads,
        })
    }
    /// The trusted clock of the last committed record; zero before any record.
    pub fn native_logical_time(&self) -> u64 {
        match self.state.rows.get(&Key::Meta) {
            Some(Row::Meta(meta)) => meta.logical_time,
            _ => 0,
        }
    }
    /// Frozen V1 bytes retained by import (23 §5.2), never reinterpreted here.
    pub fn native_legacy_bytes(&self, key: NativeLegacyKey) -> Option<&[u8]> {
        let key = match key {
            NativeLegacyKey::Testament(id) => Key::LegacyTestament(id),
            NativeLegacyKey::EvidenceSet(id) => Key::LegacyEvidenceSet(id),
            NativeLegacyKey::Run { validation, run } => Key::LegacyRun(validation, run),
            NativeLegacyKey::Definition(id) => Key::LegacyDefinition(id),
        };
        match self.state.rows.get(&key) {
            Some(
                Row::LegacyTestament(row)
                | Row::LegacyEvidenceSet(row)
                | Row::LegacyRun(row)
                | Row::LegacyDefinition(row),
            ) => Some(row.bytes()),
            _ => None,
        }
    }
    pub fn native_claim(&self, id: ClaimId) -> Option<&ClaimState> {
        as_claim(self.state.row(&Key::Claim(id)))
    }
    /// The outcome recorded for `request`, a spilled one read back (doc 31 M3): an outcome that
    /// cannot be read is an error, never taken for none.
    pub fn native_outcome(
        &self,
        request: impl Into<NativeInvocation>,
    ) -> Result<Option<NativeOutcome>, NativeError> {
        let invocation = request.into();
        as_outcome(
            self.state.rows.get(&Key::Outcome(invocation)),
            self.state.ledger,
            invocation,
            self.state.payloads(),
        )
    }
    /// Every committed native outcome (the retry-dedup records that survive a
    /// checkpoint) in native-sequence order — one per committed record. The
    /// offline publications reader projects these to the linearizability
    /// checker's receipts; each carries its `invocation`, `sequence` and
    /// `intent`.
    pub fn native_outcomes(&self) -> Result<Vec<NativeOutcome>, NativeError> {
        let mut outcomes: Vec<NativeOutcome> = self
            .state
            .rows
            .entries()
            .filter_map(|entry| match (&entry.key, &entry.value) {
                (Key::Outcome(invocation), Row::Outcome(outcome)) => {
                    Some(outcome.expand(self.state.ledger, *invocation, self.state.payloads()))
                }
                _ => None,
            })
            .collect::<Result<_, _>>()?;
        outcomes.sort_by_key(|outcome| outcome.sequence.0);
        Ok(outcomes)
    }
    /// The continuation of a claim that retired to the archive (26 §4).
    pub fn native_retired(&self, id: ClaimId) -> Option<&RetiredClaim> {
        match self.state.rows.get(&Key::Retired(id)) {
            Some(Row::Retired(value)) => Some(value),
            _ => None,
        }
    }
    /// The content roots the committed rows name (26 §5): every artifact
    /// held as a content object and every continuation's bundle, walked at
    /// most `max_visits` rows at a time from `cursor` (exclusive) on. The
    /// page's `next` resumes the walk; `None` once every row was visited.
    pub fn native_content_roots(
        &self,
        cursor: Option<NativeRowCursor>,
        max_visits: usize,
    ) -> Result<ContentRootsPage, NativeError> {
        let mut roots = Vec::new();
        let mut visited = 0usize;
        let mut last = None;
        let mut truncated = false;
        let mut push = |root: ContentRoot| -> Result<(), NativeError> {
            roots
                .try_reserve_exact(1)
                .map_err(|_| NativeError::Capacity("content roots"))?;
            roots.push(root);
            Ok(())
        };
        let mut walk = |entry: ranges::RowEntry<'_>| -> Result<bool, NativeError> {
            if visited >= max_visits {
                return Ok(false);
            }
            visited = visited.saturating_add(1);
            last = Some(entry.key);
            match (&entry.key, entry.value) {
                (Key::Artifact(id), Row::Artifact(owned)) => {
                    use focal_model::lifecycle::artifact_descriptor::PayloadSpec;
                    if let Some(artifact) = owned.get() {
                        match artifact.descriptor().payload() {
                            PayloadSpec::Content(pointer) => {
                                push(ContentRoot::Artifact {
                                    artifact: *id,
                                    pointer,
                                })?;
                            }
                            PayloadSpec::Inline(_) => {
                                // An inline payload was sealed as an object
                                // at admission; the row's custody names it.
                                push(ContentRoot::Inline {
                                    artifact: *id,
                                    pointer: artifact.custody().payload(),
                                })?;
                            }
                        }
                    }
                }
                // A continuation claiming its own retirement's sequence is a cold family's
                // (doc 31 M2, A2); one claiming less names a custody bundle.
                (Key::Retired(claim), Row::Retired(value)) if value.through == value.retired_at => {
                    push(ContentRoot::Cold {
                        claim: *claim,
                        digest: value.bundle,
                        bytes: value.bytes,
                        retired_at: value.retired_at,
                    })?
                }
                (Key::Retired(claim), Row::Retired(value)) => push(ContentRoot::Bundle {
                    claim: *claim,
                    root: value.bundle,
                    bytes: value.bytes,
                })?,
                // A closing seal's row names no bundle (F12 level 5).
                (Key::Seal(ordinal), Row::Seal(value)) if !value.is_closing(*ordinal) => {
                    push(ContentRoot::Seal {
                        ordinal: *ordinal,
                        root: value.bundle,
                        bytes: value.bytes,
                    })?
                }
                _ => {}
            }
            Ok(true)
        };
        match cursor {
            None => {
                for entry in self.state.rows.entries() {
                    if !walk(entry)? {
                        truncated = true;
                        break;
                    }
                }
            }
            Some(cursor) => {
                for entry in self.state.rows.entries_from(&cursor.0, true) {
                    if !walk(entry)? {
                        truncated = true;
                        break;
                    }
                }
            }
        }
        Ok(ContentRootsPage {
            roots,
            next: if truncated {
                last.map(NativeRowCursor)
            } else {
                None
            },
            visited,
        })
    }
    pub fn native_definition(&self, id: ValidationId) -> Option<&validation::Declaration> {
        as_definition(self.state.row(&Key::Definition(id)))
    }
    pub fn native_evaluation(&self, key: EvaluationKey) -> Option<&validation::EvaluationState> {
        as_evaluation(self.state.rows.get(&Key::Evaluation(key)))
    }
    pub fn native_receipt(&self, id: ReceiptId) -> Option<NativeReceipt> {
        as_receipt(self.state.rows.get(&Key::Receipt(id)))
    }
    pub fn native_artifact(&self, id: ArtifactId) -> Option<&NativeArtifact> {
        as_artifact(self.state.rows.get(&Key::Artifact(id)))
    }
    pub fn native_result(&self, key: NativeResultKey) -> Option<&NativeAccepted> {
        as_result(self.state.rows.get(&Key::Accepted(key)))
    }
    pub fn native_event(&self, sequence: SessionSeq, ordinal: u32) -> Option<NativeEvent> {
        match self.state.rows.get(&Key::Event(sequence, ordinal)) {
            Some(Row::Event(event)) => event.get(self.state.ledger, self.state.payloads()),
            _ => None,
        }
    }
    pub fn native_budget(&self) -> BudgetStats {
        self.state.budget.stats()
    }
    pub fn native_stats(&self) -> RangeStats {
        self.state.rows.stats()
    }
    /// The state as of the last of `pending`, candidates prepared on this core
    /// in order ([`Self::prepare_native`]'s chain): each one's changes over
    /// the ones before it over the published rows. A candidate holds its
    /// changes alone, so what it leaves of a row it does not write is read
    /// here.
    pub fn native_chain<'a>(
        &'a self,
        pending: &[&'a NativePrepared],
    ) -> Result<NativeView<'a>, NativeError> {
        Ok(NativeView(View::over(
            &self.state,
            pending_index::Chain::listed(pending)?,
        )))
    }
    /// Audits a chain of candidates a caller holds, every link: each built on the one before it,
    /// the first on the published prefix. An owner's own preparations check only its chain's head
    /// (`check_native_chain`): it builds each candidate on its tail, so the head's link is the one
    /// a publication can have moved, and a walk of every link on every preparation cost each the
    /// chain's length.
    pub fn validate_native_chain(&self, pending: &[&NativePrepared]) -> Result<(), NativeError> {
        self.state
            .rows
            .validate_chain(pending.iter().map(|item| &item.fragments))?;
        Ok(())
    }
    /// The external log owner calls this only after durability. The publication
    /// has no codec or IO and performs no allocation, including producing its
    /// result. Once it stands, the events it published are filed in the event
    /// index (doc 31 M2), which may grow; a refusal there drops the index, never
    /// the publication. Then the claims it left waiting to be delivered and the
    /// definitions it wrote are spilled where it left them (doc 31 M3,
    /// `spill_published`), which never undoes it either.
    #[allow(clippy::result_large_err)] // Return the owned candidate without allocating on refusal.
    pub fn publish_native(
        &mut self,
        prepared: NativePrepared,
    ) -> Result<NativeOutcome, NativePublishError> {
        // The rows change: nothing borrows what reads took back (doc 31 M3).
        self.state.reads.release();
        // What the rows take in a checkpoint after this mutation: what it puts, less what it
        // replaces or deletes as the committed root holds it now, every predecessor published.
        // Computed before anything is published, so a refusal leaves the root as it was.
        let encoded = match self
            .state
            .encoded_rows
            .checked_sub(prepared.encoded_removed)
            .and_then(|rest| rest.checked_add(prepared.encoded_added))
        {
            Some(encoded) => encoded,
            None => {
                return Err(NativePublishError {
                    error: MemoryError::CounterExhausted("checkpoint bytes"),
                    prepared: Some(prepared),
                });
            }
        };
        let NativePrepared {
            fragments,
            outcome,
            writes,
            encoded_added,
            encoded_removed,
            _index,
            events,
            retire,
        } = prepared;
        // A retirement's events leave with their families: unfiled while their rows still say
        // where they are filed (doc 31 M2).
        if retire.is_some() {
            self.unfile_deleted_events(&fragments);
        }
        if let Err((error, fragments)) = self.state.rows.publish_recoverable(fragments) {
            if retire.is_some() {
                self.state.events.drop_all();
            }
            return Err(NativePublishError {
                error,
                prepared: fragments.map(|fragments| NativePrepared {
                    fragments,
                    outcome,
                    writes,
                    encoded_added,
                    encoded_removed,
                    _index,
                    events,
                    retire,
                }),
            });
        }
        self.state.encoded_rows = encoded;
        self.file_events(outcome, events);
        self.spill_published(&writes);
        Ok(outcome)
    }
    /// Spills the claims a publication left waiting to be delivered, and the authored
    /// definitions it wrote, where it left them (doc 31 M3): each row's record encoding goes to
    /// the payload log and the row is rewritten where it stands to hold where its bytes are, its
    /// page's charge let go by the heap it held. A read takes one back where it touches it
    /// (`read_back`). The publication has stood, and its pages are the store's own: a row the log
    /// refuses, or one that cannot be rewritten, keeps its bytes, and nothing here undoes it. How
    /// many rows it spilled.
    fn spill_published(&mut self, writes: &mutation::WriteSet) -> usize {
        // A measurement's control: the same core with its claims held.
        #[cfg(test)]
        if std::env::var_os("FOCAL_MEASURE_HELD_CLAIMS").is_some() {
            return 0;
        }
        let NativeState {
            rows,
            payloads,
            young,
            budget,
            ..
        } = &mut self.state;
        let Some(log) = payloads.as_deref() else {
            return 0;
        };
        let mut spilled = 0usize;
        // The rows this publication wrote join the generation first, each that may spill (its
        // claim taken or tied into work since, a row stays held). One the generation cannot keep
        // spills now.
        let mut admitted = 0usize;
        for (key, deleted) in writes.entries() {
            if deleted || !matches!(key, Key::Claim(_) | Key::Definition(_)) {
                continue;
            }
            let spillable = match rows.get(&key) {
                Some(Row::Claim(claim)) => claim.spillable(),
                Some(Row::Definition(definition)) => definition.spillable(),
                _ => false,
            };
            if !spillable {
                continue;
            }
            admitted = admitted.saturating_add(1);
            let Some(now) = young.admit(key) else {
                continue;
            };
            match spill_row(rows, log, &now) {
                RowSpill::Spilled => spilled = spilled.saturating_add(1),
                RowSpill::Stays => {}
                RowSpill::PutOff => young.lost(),
            }
        }
        // Then its oldest, while it tracks more than it holds by choice or memory is short, a
        // bounded number a publication; one whose spill is put off is kept to try again.
        let mut pass = young::Pass::new(admitted);
        while let Some(oldest) = young.next(budget, &mut pass) {
            match spill_row(rows, log, &oldest) {
                RowSpill::Spilled => spilled = spilled.saturating_add(1),
                RowSpill::Stays => {}
                RowSpill::PutOff => young.put_off(oldest, &mut pass),
            }
        }
        // Rows it lost track of are found again by a walk of the rows, a bounded step a
        // publication while memory is short.
        if young.walk_due(budget) {
            spilled = spilled.saturating_add(walk_step(rows, log, young));
        }
        spilled
    }
    /// Unfile every event `fragments` deletes from the event index (doc 31 M2).
    fn unfile_deleted_events(&mut self, fragments: &ranges::Fragments) {
        let NativeState {
            rows,
            events,
            ledger,
            payloads: log,
            ..
        } = &mut self.state;
        for (_, _, change) in fragments.changes() {
            let ranges::RowChange::Delete(key) = change else {
                continue;
            };
            let Key::Event(sequence, ordinal) = key else {
                continue;
            };
            if !events.complete() {
                return;
            }
            let event = match rows.get(&key) {
                Some(Row::Event(stored)) => stored.get(*ledger, payloads::source(log)),
                _ => None,
            };
            match event {
                Some(event) => events.unfile(
                    event_index::home(event),
                    event_index::Position { sequence, ordinal },
                ),
                None => events.drop_all(),
            }
        }
    }
    /// File the events `outcome` published, every one at its sequence, on the charge its
    /// preparation funded (doc 31 M2). A publication not funded for them, or a row that is not
    /// the event it should be, lets the index go, for the rows to build again.
    fn file_events(&mut self, outcome: NativeOutcome, funding: Option<focal_memory::Allocation>) {
        let NativeState {
            rows,
            events,
            ledger,
            payloads: log,
            ..
        } = &mut self.state;
        if outcome.events == 0 || !events.complete() {
            return;
        }
        let funded = event_index::charge_for(outcome.events)
            .ok()
            .is_some_and(|wanted| {
                funding
                    .as_ref()
                    .is_some_and(|funding| funding.bytes() == wanted)
            });
        match funding {
            Some(funding) if funded => events.fund(funding),
            _ => {
                events.drop_all();
                return;
            }
        }
        for ordinal in 0..outcome.events {
            let event = match rows.get(&Key::Event(outcome.sequence, ordinal)) {
                Some(Row::Event(stored)) => stored.get(*ledger, payloads::source(log)),
                _ => None,
            };
            let Some(event) = event else {
                events.drop_all();
                return;
            };
            events.file(
                event_index::home(event),
                event_index::Position {
                    sequence: outcome.sequence,
                    ordinal,
                },
            );
        }
    }
    /// Build the event index from the committed rows where it is not built (a restored core's,
    /// or one whose memory was refused): a walk of every event, once.
    pub fn build_event_index(&mut self) {
        if !self.state.events.complete() {
            self.state.events = event_index::EventIndex::build(
                &self.state.rows,
                self.state.ledger,
                self.state.payloads(),
                &self.state.budget,
            );
        }
    }
    /// What the rows take in a checkpoint once `prepared` publishes, each replaced or deleted row
    /// looked up in the committed root: what the preparation's own count must equal.
    #[cfg(test)]
    pub(crate) fn encoded_after_by_lookup(
        &self,
        prepared: &NativePrepared,
    ) -> Result<usize, MemoryError> {
        let unencodable = |_| MemoryError::InvalidConfiguration("a committed row does not encode");
        let mut removed = 0usize;
        for (key, _) in prepared.writes.entries() {
            if let Some(row) = self.state.rows.get(&key) {
                removed = removed
                    .checked_add(
                        record_codec::checkpoint::row_bytes(
                            key,
                            row,
                            self.state.ledger,
                            self.state.payloads(),
                        )
                        .map_err(unencodable)?,
                    )
                    .ok_or(MemoryError::CounterExhausted("checkpoint bytes"))?;
            }
        }
        self.state
            .encoded_rows
            .checked_sub(removed)
            .and_then(|rest| rest.checked_add(prepared.encoded_added))
            .ok_or(MemoryError::CounterExhausted("checkpoint bytes"))
    }
    /// What the rows take in a checkpoint once `changes` are published straight to the root (a
    /// seal's): each put's row added, every touched key's committed row taken away.
    pub(in crate::native) fn encoded_after_changes(
        &self,
        changes: &[focal_memory::Change<Key, Row>],
    ) -> Result<usize, NativeError> {
        let mut removed = 0usize;
        for change in changes {
            let key = match change {
                focal_memory::Change::Put(entry) => entry.key,
                focal_memory::Change::Delete(key) => *key,
            };
            if let Some(row) = self.state.rows.get(&key) {
                removed = removed
                    .checked_add(
                        record_codec::checkpoint::row_bytes(
                            key,
                            row,
                            self.state.ledger,
                            self.state.payloads(),
                        )
                        .map_err(|_| NativeError::Capacity("a committed row does not encode"))?,
                    )
                    .ok_or(NativeError::Capacity("checkpoint bytes"))?;
            }
        }
        self.state
            .encoded_rows
            .checked_sub(removed)
            .and_then(|rest| {
                rest.checked_add(
                    encoded_added(changes.iter(), self.state.ledger, self.state.payloads()).ok()?,
                )
            })
            .ok_or(NativeError::Capacity("checkpoint bytes"))
    }
    /// What a checkpoint of the committed root encodes to, exactly: its frame and its rows.
    pub fn checkpoint_bytes(&self) -> Result<usize, NativeError> {
        record_codec::checkpoint::frame_bytes(&self.state)
            .map_err(|_| NativeError::Capacity("checkpoint frame"))?
            .checked_add(self.state.encoded_rows)
            .ok_or(NativeError::Capacity("checkpoint bytes"))
    }
    pub fn pin_native(&mut self, now: u64, ttl: u64) -> Result<NativeRead, MemoryError> {
        // A reader that cannot be made (its handles) refuses the lease, as a frozen image's does.
        let payloads = match self.state.payloads.as_deref() {
            Some(log) => Some(log.reader().map_err(|_| MemoryError::AllocationFailed)?),
            None => None,
        };
        self.state.rows.pin(now, ttl).map(|lease| NativeRead {
            lease,
            ledger: self.state.ledger,
            payloads,
            limits: self.limits,
            budget: self.state.budget.clone(),
        })
    }
    pub fn release_native(&mut self, read: &NativeRead) -> Result<(), MemoryError> {
        self.state.rows.release(&read.lease)
    }
    pub fn advance_native_clock(&mut self, now: u64) -> Result<usize, MemoryError> {
        self.state.rows.advance_clock(now)
    }
    /// The range layout the rows are held in (25 §4).
    pub fn native_layout(&self) -> &ranges::RangeLayout {
        self.state.rows.layout()
    }
    /// The member (position and identity) holding an object's rows (25 §6).
    pub fn native_member_for(&self, location: layout::NativeLocation) -> (usize, RangeId) {
        let layout = self.state.rows.layout();
        let index = layout.route_affinity(&layout::location_affinity(location));
        (
            index,
            layout.member_id(index).unwrap_or(self.state.rows.id()),
        )
    }
    /// The statistics of one member's store.
    pub fn native_member_stats(&self, index: usize) -> Option<RangeStats> {
        self.state.rows.member_stats(index)
    }
    /// An affinity dividing one member near its middle, if any (25 §8).
    pub fn native_member_split_point(&self, index: usize) -> Option<ranges::Affinity> {
        self.state.rows.member_split_point(index)
    }
    /// A digest of one member's rows at the current prefix (25 §6).
    pub fn native_member_digest(
        &self,
        index: usize,
        limits: record_codec::EncodingLimits,
    ) -> Result<ContentHash, record_codec::CodecError> {
        record_codec::checkpoint::member_digest(self, index, limits)
    }
    /// Add a range boundary at affinity `at` (a claim's affinity is its
    /// identity's bytes), naming the member from `at` on `id`. Refused
    /// under read leases, at an existing boundary, for a known identity or
    /// past the configured member bound (25 §4).
    pub fn split_native_range(
        &mut self,
        at: ranges::Affinity,
        id: RangeId,
    ) -> Result<(), NativeError> {
        self.state.reads.release();
        self.state.rows.split(
            at,
            id,
            self.limits.max_ranges,
            // A layout change applies a committed record: committed work, held
            // to what the replica holds, never refused because fresh admissions
            // filled the ordinary lane or a leader admitted under a wider share.
            focal_memory::BudgetLane::Committed,
            prepare::copy,
        )
    }
    /// Give range member `index` the durable identity `id`: what a session
    /// does once its genesis is known, so every replica names its origin
    /// member alike.
    pub fn rename_native_member(&mut self, index: usize, id: RangeId) -> Result<(), NativeError> {
        self.state.reads.release();
        self.state.rows.rename_member(index, id)
    }
    /// Remove the boundary after member `index`, joining it with the next
    /// under its identity.
    pub fn merge_native_range(&mut self, index: usize) -> Result<(), NativeError> {
        self.state.reads.release();
        self.state
            .rows
            .merge(index, focal_memory::BudgetLane::Committed)
    }
}

/// The rows a spilled claim a view reads names, read in the same view.
struct ViewRows<'a>(View<'a>);
impl record_codec::ClaimRows for ViewRows<'_> {
    fn declaration(&self, id: ValidationId) -> Option<&validation::Declaration> {
        self.0.stored_definition(id)
    }
    fn response(&self, id: TestamentId) -> Option<&focal_model::lifecycle::evidence::Response> {
        match self
            .0
            .stored_entry(&Key::Response(id))
            .map(|entry| entry.value)
        {
            Some(Row::Response(row)) => row.get(),
            _ => None,
        }
    }
    fn evaluation(&self, key: EvaluationKey) -> Option<&validation::EvaluationState> {
        as_evaluation(
            self.0
                .stored_entry(&Key::Evaluation(key))
                .map(|entry| entry.value),
        )
    }
}
fn as_claim(row: Option<&Row>) -> Option<&ClaimState> {
    match row {
        Some(Row::Claim(claim)) => claim.claim(),
        _ => None,
    }
}
/// The outcome `row` holds, a spilled one read back through `payloads`: one that cannot be read
/// is an error, never taken for none (an exact retry would run again).
fn as_outcome(
    row: Option<&Row>,
    ledger: LedgerId,
    invocation: NativeInvocation,
    payloads: Option<&dyn payloads::PayloadRead>,
) -> Result<Option<NativeOutcome>, NativeError> {
    match row {
        Some(Row::Outcome(outcome)) => outcome.expand(ledger, invocation, payloads).map(Some),
        _ => Ok(None),
    }
}
fn as_definition(row: Option<&Row>) -> Option<&validation::Declaration> {
    match row {
        Some(Row::Definition(value)) => value.get(),
        _ => None,
    }
}
fn as_evaluation(row: Option<&Row>) -> Option<&validation::EvaluationState> {
    match row {
        Some(Row::Evaluation(value)) => value.get(),
        _ => None,
    }
}
/// A state as a read sees it: the committed rows, the pending chain's changes
/// over them as of one candidate, and over those a candidate being prepared
/// (doc 30 §9). A candidate holds its changes alone, so a key's row is its
/// newest change here, else the committed row.
#[derive(Clone, Copy)]
struct View<'a> {
    state: &'a NativeState,
    chain: Option<pending_index::Chain<'a>>,
    own: Option<&'a NativePrepared>,
    /// Where this view's reads take spilled rows back: the state's own, or one a walk lets go of
    /// row by row (doc 31 M3).
    reads: Option<&'a read_back::ReadBack>,
}
impl<'a> View<'a> {
    /// This view, its spilled rows read back into `reads`, which the caller lets go of (and asks
    /// for its failures) where it is done with what was read: a walk of every row does so a row
    /// at a time, so what it reads back never outgrows one row's.
    fn with_reads(self, reads: &'a read_back::ReadBack) -> Self {
        Self {
            reads: Some(reads),
            ..self
        }
    }
    /// Where this view's reads take spilled rows back.
    fn arena(&self) -> &'a read_back::ReadBack {
        self.reads.unwrap_or(&self.state.reads)
    }
    /// Where a spilled row's bytes are read from: the committed rows' log, which every row of
    /// the chain is spilled to as well.
    pub(super) fn payloads(&self) -> Option<&'a dyn payloads::PayloadRead> {
        self.state.payloads()
    }
    /// Where a row built on this view is spilled (doc 31 M3).
    pub(super) fn payload_log(&self) -> Option<&'a dyn payloads::PayloadLog> {
        self.state.payloads.as_deref()
    }
    /// The committed rows alone.
    fn committed(state: &'a NativeState) -> Self {
        Self {
            state,
            chain: None,
            own: None,
            reads: None,
        }
    }
    /// The committed rows under `chain`.
    fn over(state: &'a NativeState, chain: Option<pending_index::Chain<'a>>) -> Self {
        Self {
            state,
            chain,
            own: None,
            reads: None,
        }
    }
    /// The committed rows with `tail`, a candidate prepared on them, over
    /// them: a fixture's view.
    #[cfg(test)]
    fn with_tail(state: &'a NativeState, tail: Option<&'a NativePrepared>) -> Self {
        Self {
            state,
            chain: None,
            own: tail,
            reads: None,
        }
    }
    /// This view with `own`, a candidate prepared on it, over it. A view that
    /// already has its own candidate takes it into its chain first: a caller's
    /// chain holds it as its newest; an owner's chain, which holds only its
    /// pending candidates, and a full caller's chain refuse.
    fn with_own(self, own: &'a NativePrepared) -> Result<Self, NativeError> {
        let chain = match (self.own, self.chain) {
            (None, chain) => chain,
            (Some(previous), None) => pending_index::Chain::listed(&[previous])?,
            (Some(previous), Some(chain)) => Some(chain.then(previous)?),
        };
        Ok(Self {
            state: self.state,
            chain,
            own: Some(own),
            reads: self.reads,
        })
    }
    /// The newest candidate this view reads: its own, else its chain's.
    fn tail(&self) -> Option<&'a NativePrepared> {
        self.own
            .or_else(|| self.chain.and_then(|chain| chain.tail()))
    }
    fn get(&self, key: Key) -> Option<&'a Row> {
        self.get_entry(&key).map(|entry| entry.value)
    }
    /// The row this view holds at `key`, whole: a spilled claim or definition read back (doc 31
    /// M3).
    fn get_entry(&self, key: &Key) -> Option<ranges::RowEntry<'a>> {
        self.stored_entry(key).map(|entry| self.read_back(entry))
    }
    /// `entry` as a read takes it: a spilled claim or definition read back, the rows a claim's
    /// encoding names read in this view.
    fn read_back(&self, entry: ranges::RowEntry<'a>) -> ranges::RowEntry<'a> {
        let value = self
            .state
            .read_back(self.arena(), &entry.key, entry.value, &ViewRows(*self));
        ranges::RowEntry { value, ..entry }
    }
    /// A definition this view holds, a spilled one read back.
    fn stored_definition(&self, id: ValidationId) -> Option<&'a validation::Declaration> {
        let entry = self.stored_entry(&Key::Definition(id))?;
        as_definition(Some(
            self.state
                .read_back_held_definition(self.arena(), entry.value),
        ))
    }
    /// The row this view holds at `key` as it is stored: a spilled one as it is spilled.
    fn stored_entry(&self, key: &Key) -> Option<ranges::RowEntry<'a>> {
        if let Some(own) = self.own {
            if !own.fragments.is_sparse() {
                // A built candidate holds its whole state.
                return own.fragments.get_entry(key);
            }
            if let Some(change) = own.fragments.change(key) {
                return change;
            }
        }
        if let Some(change) = self.chain.and_then(|chain| chain.get(key)) {
            return change;
        }
        self.state.rows.get_entry(key)
    }
    /// The outcome recorded for `invocation`, whole: one that cannot be read back is an error.
    fn outcome(&self, invocation: NativeInvocation) -> Result<Option<NativeOutcome>, NativeError> {
        as_outcome(
            self.get(Key::Outcome(invocation)),
            self.state.ledger,
            invocation,
            self.payloads(),
        )
    }
    /// Whether an outcome is recorded for `invocation`, its row's key alone: nothing read back.
    fn has_outcome(&self, invocation: NativeInvocation) -> bool {
        self.get(Key::Outcome(invocation)).is_some()
    }
    fn meta(&self) -> Meta {
        match self.get(Key::Meta) {
            Some(Row::Meta(meta)) => **meta,
            _ => Meta::default(),
        }
    }
    fn owned_claim(&self, id: ClaimId) -> Result<&OwnedClaim, NativeError> {
        match self.get(Key::Claim(id)) {
            Some(Row::Claim(row)) => Ok(row),
            _ => Err(ContractError::InvalidTarget.into()),
        }
    }
    fn definition(&self, id: ValidationId) -> Result<&validation::Declaration, NativeError> {
        as_definition(self.get(Key::Definition(id))).ok_or(ContractError::InvalidPolicy.into())
    }
    fn evaluation(&self, key: EvaluationKey) -> Result<&validation::EvaluationState, NativeError> {
        as_evaluation(self.get(Key::Evaluation(key))).ok_or(ContractError::InvalidTarget.into())
    }
    /// Ordered rows of the effective prefix at or after `start`: its own
    /// changes over the chain's over the committed rows, merged as they are
    /// walked. The caller bounds what it consumes; nothing is allocated.
    fn entries_from(&self, start: Key) -> impl Iterator<Item = ranges::RowEntry<'a>> + use<'a> {
        let built = self.own.filter(|own| !own.fragments.is_sparse());
        let whole = built.map(|own| own.fragments.entries_from(&start, false));
        let merged = built.is_none().then(|| {
            let own = self
                .own
                .into_iter()
                .flat_map(move |own| own.fragments.changes_from(&start, false))
                .map(|change| match change {
                    ranges::RowChange::Put(entry) => (entry.key, Some(entry)),
                    ranges::RowChange::Delete(key) => (key, None),
                });
            let chain = self
                .chain
                .into_iter()
                .flat_map(move |chain| chain.changes_from(&start, false));
            let committed = self
                .state
                .rows
                .entries_from(&start, false)
                .map(|entry| (entry.key, Some(entry)));
            pending_index::Overlay::new(own, pending_index::Overlay::new(chain, committed))
                .filter_map(|(_, entry)| entry)
        });
        let view = *self;
        whole
            .into_iter()
            .flatten()
            .chain(merged.into_iter().flatten())
            .map(move |entry| view.read_back(entry))
    }
    /// Committed claims whose authored `kind` relation targets `target`
    /// (the relation index of 22 §7), in identity order.
    fn relation_sources(
        &self,
        kind: focal_model::RelationKind,
        target: ClaimId,
    ) -> impl Iterator<Item = ClaimId> + use<'a> {
        let code = kind.code();
        self.entries_from(Key::ByRelation(code, target, ClaimId([0; 16])))
            .take_while(move |entry| matches!(entry.key, Key::ByRelation(k, t, _) if k == code && t == target))
            .filter(|entry| matches!(entry.value, Row::Index))
            .filter_map(|entry| match entry.key {
                Key::ByRelation(_, _, source) => Some(source),
                _ => None,
            })
    }
    /// Every evaluation of `claim` in key order with its state.
    fn evaluations_of(
        &self,
        claim: ClaimId,
    ) -> impl Iterator<Item = (EvaluationKey, &'a validation::EvaluationState)> + use<'a> {
        self.entries_from(Key::Evaluation(EvaluationKey {
            claim,
            validation: ValidationId([0; 16]),
            target: EvaluationTarget::Admission,
            generation: 0,
        }))
        .take_while(move |entry| matches!(entry.key, Key::Evaluation(key) if key.claim == claim))
        .filter_map(|entry| match (&entry.key, &entry.value) {
            (Key::Evaluation(key), Row::Evaluation(state)) => {
                state.get().map(|state| (*key, state))
            }
            _ => None,
        })
    }
}
impl EffectiveClaims for View<'_> {
    fn ledger(&self) -> LedgerId {
        self.state.ledger
    }
    fn prefix(&self) -> SessionSeq {
        self.tail()
            .map_or(SessionSeq(self.state.rows.prefix()), |tail| {
                SessionSeq(tail.fragments.prefix())
            })
    }
    fn claim(&self, id: ClaimId) -> Option<&ClaimState> {
        as_claim(self.get(Key::Claim(id)))
    }
    // Both read the parent's content in place (doc 31 M3). Its encoding was checked when it was
    // written, so a read fails only on corruption or a spilled row the payload log cannot give
    // back, where each answers what refuses: the holder's escalation, and no designation.
    fn cause_escalation(&self, parent: ClaimId) -> focal_model::Escalation {
        let mut buffer = owned::ContentBuffer::new();
        authored_reads::content_view(
            self.get(Key::ClaimContent(parent)),
            self.payloads(),
            &mut buffer,
        )
        .ok()
        .flatten()
        .and_then(|content| content.fields().policy)
        .map_or(focal_model::Escalation::Holder, |policy| policy.escalation)
    }
    fn is_designated_evaluator(&self, parent: ClaimId, actor: ParticipantId) -> bool {
        use focal_model::lifecycle::claim_descriptor::ClaimSource;
        let mut buffer = owned::ContentBuffer::new();
        let Ok(Some(content)) = authored_reads::content_view(
            self.get(Key::ClaimContent(parent)),
            self.payloads(),
            &mut buffer,
        ) else {
            return false;
        };
        content.view().requirements().any(|pin| {
            pin.is_ok_and(|pin| {
                as_definition(self.get(Key::Definition(pin.id)))
                    .is_some_and(|definition| definition.designates(actor))
            })
        })
    }
}

fn as_artifact(row: Option<&Row>) -> Option<&NativeArtifact> {
    match row {
        Some(Row::Artifact(value)) => value.get(),
        _ => None,
    }
}
fn as_result(row: Option<&Row>) -> Option<&NativeAccepted> {
    match row {
        Some(Row::Accepted(value)) => value.get(),
        _ => None,
    }
}
impl NativeRead {
    pub fn with_artifact<T>(
        &self,
        id: ArtifactId,
        now: u64,
        project: impl FnOnce(&NativeArtifact) -> T,
    ) -> Result<Option<T>, MemoryError> {
        let key = Key::Artifact(id);
        self.lease
            .project_next(&key, false, &Key::End, now, |entry| {
                if entry.key == key {
                    as_artifact(Some(entry.value)).map(project)
                } else {
                    None
                }
            })
            .map(Option::flatten)
    }
    pub fn with_result<T>(
        &self,
        id: NativeResultKey,
        now: u64,
        project: impl FnOnce(&NativeAccepted) -> T,
    ) -> Result<Option<T>, MemoryError> {
        let key = Key::Accepted(id);
        self.lease
            .project_next(&key, false, &Key::End, now, |entry| {
                if entry.key == key {
                    as_result(Some(entry.value)).map(project)
                } else {
                    None
                }
            })
            .map(Option::flatten)
    }
}

fn as_receipt(row: Option<&Row>) -> Option<NativeReceipt> {
    match row {
        Some(Row::Receipt(value)) => Some(**value),
        _ => None,
    }
}
impl NativeRead {
    pub fn receipt(&self, id: ReceiptId, now: u64) -> Result<Option<NativeReceipt>, MemoryError> {
        let key = Key::Receipt(id);
        self.lease
            .project_next(&key, false, &Key::End, now, |entry| {
                if entry.key == key {
                    as_receipt(Some(entry.value))
                } else {
                    None
                }
            })
            .map(Option::flatten)
    }
}

/// What `rows` take in a checkpoint, summed row by row: for a root built whole (a checkpoint
/// restored, an archive read), whose rows were never published one mutation at a time.
pub(in crate::native) fn encoded_rows_of(
    rows: &ranges::NativeRanges,
    ledger: LedgerId,
    payloads: Option<&dyn payloads::PayloadRead>,
) -> Result<usize, NativeError> {
    rows.entries().try_fold(0usize, |sum, entry| {
        let bytes = record_codec::checkpoint::row_bytes(entry.key, entry.value, ledger, payloads)
            .map_err(|_| NativeError::Capacity("a restored row does not encode"))?;
        sum.checked_add(bytes)
            .ok_or(NativeError::Capacity("checkpoint bytes"))
    })
}

/// What the committed rows a build replaces or deletes take in a checkpoint, counted as the build
/// takes each out of its page (`RangesPlan::build_in_replacing`): what publishing the build takes
/// away from the running total, with no row looked up again.
#[derive(Debug)]
pub(in crate::native) struct EncodedRemoved<'a> {
    ledger: LedgerId,
    /// Where a committed row's spilled bytes are read from.
    payloads: Option<&'a dyn payloads::PayloadRead>,
    bytes: usize,
}

impl<'a> EncodedRemoved<'a> {
    pub(in crate::native) fn new(
        ledger: LedgerId,
        payloads: Option<&'a dyn payloads::PayloadRead>,
    ) -> Self {
        Self {
            ledger,
            payloads,
            bytes: 0,
        }
    }
    pub(in crate::native) fn count(&mut self, key: &Key, row: &Row) -> Result<(), MemoryError> {
        let bytes = record_codec::checkpoint::row_bytes(*key, row, self.ledger, self.payloads)
            .map_err(|_| MemoryError::InvalidConfiguration("a committed row does not encode"))?;
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or(MemoryError::CounterExhausted("checkpoint bytes"))?;
        Ok(())
    }
    pub(in crate::native) fn bytes(&self) -> usize {
        self.bytes
    }
}

/// What the rows `changes` put take in a checkpoint: each put's row, as it will be written.
pub(in crate::native) fn encoded_added(
    mut changes: impl Iterator<Item = impl ranges::ChangeRead>,
    ledger: LedgerId,
    payloads: Option<&dyn payloads::PayloadRead>,
) -> Result<usize, NativeError> {
    changes.try_fold(0usize, |sum, change| match change.row() {
        Some(row) => {
            let bytes =
                record_codec::checkpoint::row_bytes(change.changed(), row, ledger, payloads)
                    .map_err(|_| NativeError::Capacity("a row that does not encode"))?;
            sum.checked_add(bytes)
                .ok_or(NativeError::Capacity("checkpoint bytes"))
        }
        None => Ok(sum),
    })
}

/// One step of the walk that finds again the rows a young generation lost track of (doc 31 M3):
/// from where the last stopped, [`young::WALK_VISITS`] rows at most, every held row it meets
/// that may spill spilled ([`young::WALK_SPILLS`] at most), one put off counted to be found by a
/// later walk. What it spilled.
fn walk_step(
    rows: &mut ranges::NativeRanges,
    log: &dyn payloads::PayloadLog,
    young: &mut young::Young,
) -> usize {
    let mut found = [None; young::WALK_SPILLS];
    let (count, last, ended) = match young.walk_from() {
        Some(after) => walk_visit(rows.entries_from(&after, true), &mut found),
        None => walk_visit(rows.entries(), &mut found),
    };
    young.walked(if ended { None } else { last });
    let mut spilled = 0usize;
    for tracked in found.iter().take(count).flatten() {
        match spill_row(rows, log, &tracked.key()) {
            RowSpill::Spilled => spilled = spilled.saturating_add(1),
            RowSpill::Stays => {}
            RowSpill::PutOff => young.found_put_off(),
        }
    }
    spilled
}

/// The held rows that may spill among `entries`, into `found`: how many, the last row visited,
/// and whether the rows ended before the step's bounds did.
fn walk_visit<'a>(
    entries: impl Iterator<Item = ranges::RowEntry<'a>>,
    found: &mut [Option<young::Tracked>; young::WALK_SPILLS],
) -> (usize, Option<Key>, bool) {
    let mut count = 0usize;
    let mut last = None;
    for (visited, entry) in entries.enumerate() {
        if visited >= young::WALK_VISITS || count >= young::WALK_SPILLS {
            return (count, last, false);
        }
        last = Some(entry.key);
        let spillable = match entry.value {
            Row::Claim(claim) => claim.spillable(),
            Row::Definition(definition) => definition.spillable(),
            _ => false,
        };
        if spillable
            && let Some(tracked) = young::Tracked::of(entry.key)
            && let Some(slot) = found.get_mut(count)
        {
            *slot = Some(tracked);
            count = count.saturating_add(1);
        }
    }
    (count, last, true)
}

/// What spilling one row where it stands came to ([`spill_row`]).
enum RowSpill {
    Spilled,
    /// Held for what it is: another claim depends on it, it may not spill, its encoding is wider
    /// than a spilled row's, or it is gone. Only a change to it alters that.
    Stays,
    /// Put off: its page is shared with a frozen image or a lease, or the log refuses payloads
    /// for now.
    PutOff,
}

/// Spill the row at `key` where it stands (doc 31 M3): its payload appended to `log` only once its
/// page is the store's own, so a page shared with a frozen image or a lease costs the log nothing.
fn spill_row(
    rows: &mut ranges::NativeRanges,
    log: &dyn payloads::PayloadLog,
    key: &Key,
) -> RowSpill {
    // A claim another depends on is reached by work on that one: it stays held.
    if let Key::Claim(id) = key
        && rows.get(&Key::IncomingHead(*id)).is_some()
    {
        return RowSpill::Stays;
    }
    let mut refused = false;
    let rewritten = rows.rewrite(key, |row| {
        let spill = match row {
            Row::Claim(claim) => claim.spill(log).map(|spill| match spill {
                payloads::Spill::Spilled(claim) => payloads::Spill::Spilled(Row::Claim(claim)),
                payloads::Spill::Stays => payloads::Spill::Stays,
                payloads::Spill::Refused => payloads::Spill::Refused,
            }),
            Row::Definition(definition) => definition.spill(log).map(|spill| match spill {
                payloads::Spill::Spilled(definition) => {
                    payloads::Spill::Spilled(Row::Definition(definition))
                }
                payloads::Spill::Stays => payloads::Spill::Stays,
                payloads::Spill::Refused => payloads::Spill::Refused,
            }),
            _ => Ok(payloads::Spill::Stays),
        };
        Ok(match spill {
            Ok(payloads::Spill::Spilled(row)) => Some((row, 0)),
            Ok(payloads::Spill::Refused) => {
                refused = true;
                None
            }
            Ok(payloads::Spill::Stays) | Err(_) => None,
        })
    });
    match rewritten {
        Ok(focal_memory::Rewrite::Rewritten) => RowSpill::Spilled,
        Ok(focal_memory::Rewrite::Shared) => RowSpill::PutOff,
        Ok(focal_memory::Rewrite::Unchanged) if refused => RowSpill::PutOff,
        Ok(focal_memory::Rewrite::Unchanged | focal_memory::Rewrite::Absent) => RowSpill::Stays,
        // The store refused to look: tried again, within a publication's bound.
        Err(_) => RowSpill::PutOff,
    }
}
