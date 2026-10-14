//! Retirement to the archive (26 §4). The unit is a family: a claim, the
//! claims it owns, and everything registered under them — their
//! declarations, evaluations, results, cycles, receipts, responses,
//! testaments, work and diagnostic artifacts, monitors, index rows and
//! events. A family is eligible only when every member is terminal and
//! released, nothing outside it still refers to a member (no dependent
//! link, relation, monitor or artifact input from a live claim, and no live
//! parent), so once its rows leave the core no validator or reader can
//! reach a hole. Every replica derives the same family from the same
//! committed state, so a retirement record naming the root applies alike.
//! Outcome and creation-result rows stay: every native sequence keeps its
//! one outcome, and an exact retry keeps finding it.
use super::*;
use focal_memory::{Allocation, BudgetKind, BudgetLane, Change};
use focal_model::Cause;
use focal_model::lifecycle::artifact_descriptor::PayloadSpec;
use std::collections::BTreeSet;

/// The most claims one family may hold.
pub const MAX_FAMILY_MEMBERS: usize = 64;
/// The most rows one family may take to the archive.
pub const MAX_FAMILY_ROWS: usize = 65_536;
/// The most diagnostics chained under one cycle that are followed.
const MAX_CHAIN: usize = 4096;

/// Why a family cannot leave the core now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetirementRefusal {
    /// The root is not a live claim.
    Unknown(ClaimId),
    NotTerminal(ClaimId),
    NotReleased(ClaimId),
    /// The root is still owned by a live claim.
    LiveParent(ClaimId),
    /// A live claim outside the family depends on, relates to or monitors
    /// the member.
    LiveDependent {
        member: ClaimId,
        by: ClaimId,
    },
    /// A member's own monitor watches a claim outside the family.
    LiveMonitor {
        member: ClaimId,
        monitor: MonitorId,
    },
    /// An artifact outside the family took the member as an input.
    LiveReference {
        member: ClaimId,
        artifact: ArtifactId,
    },
    /// A member's evaluation has begun and is neither terminal nor fenced:
    /// the owner still holds its completion contract.
    LiveEvaluation {
        member: ClaimId,
        evaluation: EvaluationKey,
    },
    TooLarge,
    /// The outcome the retirement publishes would take the core past its
    /// outcome bound, the bound checkpoint recovery enforces: the state it
    /// made could not be restored under the configuration that made it.
    OutcomeCapacity,
    /// The outcome is promised to a live report, or is the one control
    /// outcome the owner keeps for an authority decision; it frees as the
    /// reports arrive.
    OutcomesReserved,
    /// The committed rows contradict themselves.
    Corrupt,
    /// A member's spilled row could not be read back from the replica's payload log (doc 31 M3).
    PayloadUnavailable,
}

/// The rows a family takes to the archive, in key order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetirementFamily {
    pub root: ClaimId,
    /// The root first, then every owned claim in discovery order.
    pub members: Vec<ClaimId>,
    /// Event rows leaving with each member, in `members` order; events of
    /// rows without a member affinity count against the root.
    pub events: Vec<u32>,
    /// The greatest sequence any of the family's events carries: the bundle
    /// must claim at least this much, and the retention floor must have
    /// passed it (26 §4).
    pub through: SessionSeq,
    /// The content roots of the family's artifacts held as content objects:
    /// the proof the bundle's header names so custody keeps it (26 §5).
    pub content: Vec<ContentHash>,
    /// The content roots of the objects every replica sealed at admission
    /// for the family's artifacts held inline; the header names them too.
    pub inline: Vec<ContentHash>,
    pub(super) keys: Vec<Key>,
}
impl RetirementFamily {
    pub fn rows(&self) -> usize {
        self.keys.len()
    }
}
/// The bytes of a cold batch's count, before its entries (u16, little-endian).
pub const COLD_COUNT_BYTES: usize = 2;
/// The bytes of one family's entry in a cold batch's header: its root, its frame's digest and
/// its frame's length (u32, little-endian).
pub const COLD_ENTRY_BYTES: usize = 52;
/// One family's entry in a cold batch (doc 31 M2, A2): its root, and its frame's digest and
/// length.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColdEntry {
    pub root: ClaimId,
    pub digest: ContentHash,
    pub length: u32,
}
/// A retirement's cold batch ([`Core::cold_batch`]): the count, each family's entry, then each
/// family's archive frame in the entries' order: what every replica appends to its cold stream
/// where the record applies. Charged while held.
#[derive(Debug)]
pub struct ColdBatch {
    pub bytes: Vec<u8>,
    pub entries: Vec<ColdEntry>,
    _charge: Allocation,
}
/// A family's frame past what a frame may hold, or memory refused it.
fn cold_codec(error: record_codec::CodecError) -> NativeError {
    match error {
        record_codec::CodecError::Allocation => NativeError::Memory(MemoryError::AllocationFailed),
        _ => NativeError::Capacity("cold frame"),
    }
}
/// A cold batch's header, read from its first bytes: its entries, and where its frames begin.
pub fn cold_header(bytes: &[u8]) -> Result<(Vec<ColdEntry>, usize), NativeError> {
    let count: [u8; COLD_COUNT_BYTES] = bytes
        .get(..COLD_COUNT_BYTES)
        .and_then(|count| count.try_into().ok())
        .ok_or(NativeError::Capacity("cold batch header"))?;
    let count = usize::from(u16::from_le_bytes(count));
    let frames = count
        .checked_mul(COLD_ENTRY_BYTES)
        .and_then(|n| n.checked_add(COLD_COUNT_BYTES))
        .ok_or(NativeError::Capacity("cold batch header"))?;
    let table = bytes
        .get(COLD_COUNT_BYTES..frames)
        .ok_or(NativeError::Capacity("cold batch header"))?;
    let mut entries = Vec::new();
    entries
        .try_reserve_exact(count)
        .map_err(|_| MemoryError::AllocationFailed)?;
    let (table, rest) = table.as_chunks::<COLD_ENTRY_BYTES>();
    if !rest.is_empty() {
        return Err(NativeError::Capacity("cold batch header"));
    }
    for entry in table {
        let entry_error = || NativeError::Capacity("cold batch entry");
        let (root, entry) = entry.split_first_chunk::<16>().ok_or_else(entry_error)?;
        let (digest, entry) = entry.split_first_chunk::<32>().ok_or_else(entry_error)?;
        let (length, _) = entry.split_first_chunk::<4>().ok_or_else(entry_error)?;
        entries.push(ColdEntry {
            root: ClaimId(*root),
            digest: ContentHash(*digest),
            length: u32::from_le_bytes(*length),
        });
    }
    Ok((entries, frames))
}

/// Where a candidate walk resumes: the status bucket and the last claim
/// visited in it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetirementCursor {
    pub status: ClaimStatus,
    pub after: Option<ClaimId>,
}
/// A bounded walk over the terminal status buckets for retirement
/// candidates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetirementCandidates {
    /// Terminal, released claims found within the visit bound.
    pub claims: Vec<ClaimId>,
    /// Where the next walk resumes; `None` once every bucket is exhausted.
    pub next: Option<RetirementCursor>,
}

/// What the archive frame quotes before it is written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchiveQuote {
    pub bytes: usize,
    pub visits: usize,
    pub hash: ContentHash,
}

/// Whether a row under a claim's affinity is the claim's own: keyed by the
/// claim, by one of its cycles, evaluations or results, by a link or
/// relation into it, or an index row whose primary is the claim or one of
/// its evaluations.
fn names_member(key: Key, member: ClaimId) -> bool {
    match key {
        Key::IncomingHead(c)
        | Key::IncomingLink(c, _)
        | Key::MonitorHead(c)
        | Key::MonitorLink(c, _)
        | Key::Claim(c)
        | Key::RetiredCycleHead(c)
        | Key::ClaimResultTestament(c)
        | Key::ClaimContent(c)
        | Key::Retired(c)
        | Key::ByRelation(_, c, _) => c == member,
        Key::Cycle(cycle) | Key::RetiredCycle(cycle) | Key::WorkSlot(cycle, _) => {
            cycle.claim == member
        }
        Key::Evaluation(evaluation) => evaluation.claim == member,
        Key::MissingResult(result) | Key::Accepted(result) | Key::DeliveryResult(result) => {
            result.evaluation.claim == member
        }
        Key::ArtifactInput(object, _) => object.0 == member.0,
        // A timer's outcome is its claim's (F12); a request's never.
        Key::Outcome(NativeInvocation::ClaimDeadline(key)) => key.claim == member,
        Key::Outcome(NativeInvocation::MonitorDeadline(key)) => key.claim == member,
        Key::Outcome(NativeInvocation::EvaluationDeadline(key)) => key.evaluation.claim == member,
        Key::Outcome(_) | Key::CreationResult(_) => false,
        other => match index_rows::primary(other) {
            Some(index_rows::Primary::Claim(claim)) => claim == member,
            Some(index_rows::Primary::Evaluation(evaluation)) => evaluation.claim == member,
            _ => false,
        },
    }
}

fn predicate_target(root: &WaitPredicate) -> ClaimId {
    match root {
        WaitPredicate::Satisfied(id)
        | WaitPredicate::Terminal(id)
        | WaitPredicate::Released(id) => *id,
    }
}

struct Closure<'a> {
    core: &'a Core<NativeState>,
    members: Vec<ClaimId>,
    keys: BTreeSet<Key>,
    artifacts: BTreeSet<ArtifactId>,
    testaments: BTreeSet<TestamentId>,
    receipts: BTreeSet<ReceiptId>,
    definitions: BTreeSet<ValidationId>,
    monitors: BTreeSet<MonitorId>,
    inputs: Vec<(ClaimId, ArtifactId)>,
}
impl Closure<'_> {
    fn member(&self, claim: ClaimId) -> bool {
        self.members.contains(&claim)
    }
    fn keep(&mut self, key: Key) -> Result<(), RetirementRefusal> {
        if self.keys.len() >= MAX_FAMILY_ROWS {
            return Err(RetirementRefusal::TooLarge);
        }
        self.keys.insert(key);
        Ok(())
    }
    fn keep_existing(&mut self, key: Key) -> Result<(), RetirementRefusal> {
        if self.core.state.rows.get(&key).is_some() {
            self.keep(key)?;
        }
        Ok(())
    }
    /// Every row under one affinity that names the member, with the objects
    /// they lead to. An affinity is a bucket: a row of another object that
    /// happens to share it (a participant's index rows, a colliding id) is
    /// not the member's and stays.
    fn scan_member(&mut self, member: ClaimId) -> Result<(), RetirementRefusal> {
        let first = Key::IncomingHead(member);
        let mut found = Vec::new();
        for entry in self.core.state.rows.entries_from(&first, false) {
            if layout::affinity(&entry.key) != member.0 {
                break;
            }
            if !names_member(entry.key, member) {
                continue;
            }
            if found.len() >= MAX_FAMILY_ROWS {
                return Err(RetirementRefusal::TooLarge);
            }
            found.push(entry.key);
            match (entry.key, &entry.value) {
                (Key::IncomingLink(_, source), _) | (Key::ByRelation(_, _, source), _) => {
                    if !self.member(source) {
                        return Err(RetirementRefusal::LiveDependent { member, by: source });
                    }
                }
                (Key::MonitorLink(_, id), Row::MonitorLink(link)) => {
                    let owner = match link {
                        Some(link) => link.owner,
                        None => match self.core.state.rows.get(&Key::Monitor(id)) {
                            Some(Row::Monitor(allocation)) => ClaimId(allocation.owner.object.0),
                            _ => return Err(RetirementRefusal::Corrupt),
                        },
                    };
                    if !self.member(owner) {
                        return Err(RetirementRefusal::LiveDependent { member, by: owner });
                    }
                }
                (Key::ArtifactInput(_, artifact), _) => self.inputs.push((member, artifact)),
                (Key::Evaluation(evaluation), Row::Evaluation(owned)) => {
                    let state = owned.get().ok_or(RetirementRefusal::Corrupt)?;
                    if state.has_begun() && !state.state().is_terminal() && state.fence().is_none()
                    {
                        return Err(RetirementRefusal::LiveEvaluation { member, evaluation });
                    }
                }
                (Key::Cycle(cycle) | Key::RetiredCycle(cycle) | Key::WorkSlot(cycle, _), value) => {
                    self.receipts.insert(cycle.receipt);
                    match value {
                        Row::Cycle(row) => {
                            if let Some(testament) = row.response {
                                self.testaments.insert(testament);
                            }
                            self.chain(row.diagnostic_head)?;
                        }
                        Row::WorkSlot(artifact) => {
                            self.artifacts.insert(*artifact);
                        }
                        _ => {}
                    }
                }
                (Key::Accepted(_), Row::Accepted(accepted)) => {
                    let accepted = accepted.get().ok_or(RetirementRefusal::Corrupt)?;
                    self.artifacts.insert(accepted.artifact().reference().id);
                }
                (Key::ClaimResultTestament(_), Row::ClaimResultTestament(testament)) => {
                    self.testaments.insert(*testament);
                }
                (Key::Claim(_), Row::Claim(owned)) => {
                    let claim = owned.claim().ok_or(RetirementRefusal::Corrupt)?;
                    // Definitions declared at creation live in the acceptance
                    // policy; later registrations live in the registration set.
                    // Both hydrate from their claim on restore, so both leave
                    // with it.
                    for declaration in claim.acceptance().declarations() {
                        self.definitions
                            .insert(ValidationId(declaration.binding().object.0));
                    }
                    if let Some(registrations) = owned.registrations() {
                        for registration in registrations.rows() {
                            self.definitions
                                .insert(ValidationId(registration.binding().object.0));
                        }
                    }
                    for scope in claim.scopes().iter() {
                        self.monitors.insert(scope.id());
                        for root in scope.roots() {
                            if !self.member(predicate_target(root)) {
                                return Err(RetirementRefusal::LiveMonitor {
                                    member,
                                    monitor: scope.id(),
                                });
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        for key in found {
            self.keep(key)?;
        }
        Ok(())
    }
    fn chain(&mut self, head: Option<ArtifactId>) -> Result<(), RetirementRefusal> {
        let mut next = head;
        for _ in 0..MAX_CHAIN {
            let Some(id) = next else {
                return Ok(());
            };
            self.artifacts.insert(id);
            next = match self.core.state.rows.get(&Key::Diagnostic(id)) {
                Some(Row::Diagnostic(row)) => row.get().and_then(|row| row.next),
                _ => None,
            };
        }
        Err(RetirementRefusal::TooLarge)
    }
    /// Every row under an object's own affinity.
    fn scan_affinity(&mut self, first: Key, affinity: [u8; 16]) -> Result<(), RetirementRefusal> {
        let mut found = Vec::new();
        for entry in self.core.state.rows.entries_from(&first, false) {
            if layout::affinity(&entry.key) != affinity {
                break;
            }
            // An outcome under an object's affinity is one of its timers'
            // (F12): it leaves with the family, into the bundle, since
            // nothing re-delivers a retired object's timer. Requests'
            // outcomes sit under their principal, never here.
            if matches!(entry.key, Key::CreationResult(_))
                || matches!(entry.key, Key::Outcome(NativeInvocation::Request(_)))
            {
                continue;
            }
            if found.len() >= MAX_FAMILY_ROWS {
                return Err(RetirementRefusal::TooLarge);
            }
            found.push(entry.key);
        }
        for key in found {
            self.keep(key)?;
        }
        Ok(())
    }
    fn index_keys(
        &mut self,
        emit: impl FnOnce(
            &mut dyn FnMut(index_rows::IndexChange) -> Result<(), NativeError>,
        ) -> Result<(), NativeError>,
    ) -> Result<(), RetirementRefusal> {
        let mut keys = Vec::new();
        emit(&mut |change| {
            if keys.len() >= MAX_FAMILY_ROWS {
                return Err(NativeError::Capacity("family index rows"));
            }
            keys.push(change.key());
            Ok(())
        })
        .map_err(|_| RetirementRefusal::Corrupt)?;
        for key in keys {
            self.keep_existing(key)?;
        }
        Ok(())
    }
}

impl Core<NativeState> {
    /// Whether the outcome a retirement publishes fits the core's outcome
    /// bound, the bound checkpoint recovery enforces, as every outcome
    /// ordinary admission publishes must: the first thing a family
    /// derivation asks, before a row is walked, and what a session asks
    /// before it derives one. One row read.
    pub fn check_retirement_outcome(&self) -> Result<(), RetirementRefusal> {
        let outcomes = match self.state.rows.get(&Key::Meta) {
            Some(Row::Meta(meta)) => meta.outcomes.saturating_sub(meta.sealed),
            _ => return Err(RetirementRefusal::Corrupt),
        };
        if outcomes
            .checked_add(1)
            .is_none_or(|next| next > self.limits.outcomes)
        {
            return Err(RetirementRefusal::OutcomeCapacity);
        }
        Ok(())
    }
    /// The family rooted at `root` and every row it takes to the archive,
    /// or why it cannot leave yet. Deterministic over the committed rows.
    /// Refused first, before a row is walked, when the outcome the
    /// retirement publishes would pass the outcome bound
    /// (`OutcomeCapacity`).
    pub fn retirement_family(&self, root: ClaimId) -> Result<RetirementFamily, RetirementRefusal> {
        self.check_retirement_outcome()?;
        let mut closure = Closure {
            core: self,
            members: Vec::new(),
            keys: BTreeSet::new(),
            artifacts: BTreeSet::new(),
            testaments: BTreeSet::new(),
            receipts: BTreeSet::new(),
            definitions: BTreeSet::new(),
            monitors: BTreeSet::new(),
            inputs: Vec::new(),
        };
        // Members: the root and every claim it owns, each terminal and
        // released; the root under no live parent.
        let mut pending = vec![root];
        while let Some(member) = pending.pop() {
            if closure.members.contains(&member) {
                continue;
            }
            if closure.members.len() >= MAX_FAMILY_MEMBERS {
                return Err(RetirementRefusal::TooLarge);
            }
            let claim = as_claim(self.state.rows.get(&Key::Claim(member)))
                .ok_or(RetirementRefusal::Unknown(member))?;
            if !claim.is_terminal() {
                return Err(RetirementRefusal::NotTerminal(member));
            }
            if !claim.released() {
                return Err(RetirementRefusal::NotReleased(member));
            }
            if member == root
                && let Cause::Claim(parent) = claim.lineage().cause()
                && self.state.rows.get(&Key::Claim(*parent)).is_some()
            {
                return Err(RetirementRefusal::LiveParent(*parent));
            }
            closure.members.push(member);
            for child in claim.scopes().children() {
                pending.push(child.id());
            }
        }
        for member in closure.members.clone() {
            closure.scan_member(member)?;
        }
        // Dependents by identity: artifacts, testaments, receipts,
        // declarations and monitor allocations.
        let mut content: Vec<ContentHash> = Vec::new();
        let mut inline: Vec<ContentHash> = Vec::new();
        for artifact in closure.artifacts.clone() {
            closure.scan_affinity(Key::Artifact(artifact), artifact.0)?;
            if let Some(native) = self.native_artifact(artifact) {
                let descriptor = native.descriptor();
                closure.index_keys(|sink| index_rows::artifact(descriptor, sink))?;
                // Content identities live under their own affinity and name
                // the object they resolve to; they leave with it.
                closure.keep_existing(Key::ArtifactIdentity(descriptor.content_hash()))?;
                match descriptor.payload() {
                    PayloadSpec::Content(pointer) => {
                        if !content.contains(&pointer.root) {
                            content
                                .try_reserve_exact(1)
                                .map_err(|_| RetirementRefusal::TooLarge)?;
                            content.push(pointer.root);
                        }
                    }
                    PayloadSpec::Inline(_) => {
                        let root = native.custody().payload().root;
                        if !inline.contains(&root) {
                            inline
                                .try_reserve_exact(1)
                                .map_err(|_| RetirementRefusal::TooLarge)?;
                            inline.push(root);
                        }
                    }
                }
            }
        }
        content.sort();
        inline.sort();
        for testament in closure.testaments.clone() {
            closure.scan_affinity(Key::Response(testament), testament.0)?;
        }
        for receipt in closure.receipts.clone() {
            closure.keep_existing(Key::Receipt(receipt))?;
        }
        for definition in closure.definitions.clone() {
            closure.scan_affinity(Key::Definition(definition), definition.0)?;
            if let Some(Row::Definition(owned)) = self.state.rows.get(&Key::Definition(definition))
            {
                if let Some(declaration) = owned.get() {
                    closure.index_keys(|sink| index_rows::definition(declaration, sink))?;
                }
                if let Some(descriptor) = owned.descriptor() {
                    closure.keep_existing(Key::DefinitionIdentity(
                        descriptor.schema(),
                        descriptor.content_hash(),
                    ))?;
                }
            }
        }
        for monitor in closure.monitors.clone() {
            closure.keep_existing(Key::Monitor(monitor))?;
        }
        for (member, artifact) in closure.inputs.clone() {
            if !closure.artifacts.contains(&artifact) {
                return Err(RetirementRefusal::LiveReference { member, artifact });
            }
        }
        // Index rows of the claims and their results.
        let mut buffer = crate::native::owned::ContentBuffer::new();
        for member in closure.members.clone() {
            let (claim, content) = match self.state.rows.get(&Key::Claim(member)) {
                Some(Row::Claim(owned)) => {
                    let claim = owned.claim().ok_or(RetirementRefusal::Corrupt)?;
                    let content = match self.state.rows.get(&Key::ClaimContent(member)) {
                        Some(Row::ClaimContent(content)) => Some(content),
                        _ => None,
                    };
                    (claim, content)
                }
                _ => return Err(RetirementRefusal::Corrupt),
            };
            // The content read in place from its encoding (doc 31 M3), a spilled one read back.
            let content = content
                .map(|content| content.read(self.state.payloads(), &mut buffer))
                .transpose()
                .map_err(|error| match error {
                    NativeError::Payload(_) => RetirementRefusal::PayloadUnavailable,
                    _ => RetirementRefusal::Corrupt,
                })?;
            let view = content
                .map(|content| content.view())
                .transpose()
                .map_err(|_| RetirementRefusal::Corrupt)?;
            closure.index_keys(|sink| {
                index_rows::claim(
                    None,
                    claim,
                    view.as_ref()
                        .map(|view| view as &dyn index_rows::ContentSource),
                    &index_rows::NeverConsumed,
                    sink,
                )
            })?;
            if let Some(content) = content {
                let schema = content.schema().ok_or(RetirementRefusal::Corrupt)?;
                let hash = content.content_hash().ok_or(RetirementRefusal::Corrupt)?;
                closure.keep_existing(Key::ClaimIdentity(schema, hash))?;
            }
            let first = Key::Accepted(NativeResultKey {
                evaluation: EvaluationKey {
                    claim: member,
                    validation: ValidationId::from_u128(0),
                    target: EvaluationTarget::Admission,
                    generation: 0,
                },
                revision: focal_model::ObjectRevision(0),
            });
            let mut accepted = Vec::new();
            for entry in self.state.rows.entries_from(&first, false) {
                if layout::affinity(&entry.key) != member.0 {
                    break;
                }
                if let (Key::Accepted(key), Row::Accepted(value)) = (entry.key, &entry.value) {
                    let value = value.get().ok_or(RetirementRefusal::Corrupt)?;
                    accepted.push((key, value.artifact().result().snapshot_v1().verdict));
                }
            }
            for (key, verdict) in accepted {
                closure.index_keys(|sink| index_rows::accepted(key, verdict, sink))?;
            }
        }
        // Events of everything above, attributed to the member whose rows
        // they describe: read from the event index under each object's home
        // (doc 31 M2), in proportion to the family; where the index is not
        // built, by a walk of every event, which finds the same.
        let mut events = Vec::new();
        let mut through = SessionSeq(0);
        let mut counts = Vec::new();
        counts
            .try_reserve_exact(closure.members.len())
            .map_err(|_| RetirementRefusal::TooLarge)?;
        counts.extend(closure.members.iter().map(|_| 0u32));
        let mut consider = |key: Key, stored: &OwnedEvent| -> Result<(), RetirementRefusal> {
            let event = stored
                .get(self.state.ledger, self.state.payloads())
                .ok_or(RetirementRefusal::Corrupt)?;
            let object = record_codec::event_object(event);
            if closure.keys.contains(&object) {
                if events.len() >= MAX_FAMILY_ROWS {
                    return Err(RetirementRefusal::TooLarge);
                }
                events.push(key);
                through = through.max(event.sequence);
                let owner = ClaimId(layout::affinity(&object));
                let slot = closure
                    .members
                    .iter()
                    .position(|member| *member == owner)
                    .unwrap_or(0);
                let count = counts.get_mut(slot).ok_or(RetirementRefusal::Corrupt)?;
                *count = count.checked_add(1).ok_or(RetirementRefusal::TooLarge)?;
            }
            Ok(())
        };
        if self.state.events.complete() {
            let homes: BTreeSet<[u8; 16]> = closure
                .members
                .iter()
                .map(|member| member.0)
                .chain(closure.artifacts.iter().map(|artifact| artifact.0))
                .chain(closure.testaments.iter().map(|testament| testament.0))
                .chain(closure.definitions.iter().map(|definition| definition.0))
                .collect();
            for home in homes {
                for position in self.state.events.positions(home) {
                    let key = Key::Event(position.sequence, position.ordinal);
                    let Some(Row::Event(stored)) = self.state.rows.get(&key) else {
                        return Err(RetirementRefusal::Corrupt);
                    };
                    consider(key, stored)?;
                }
            }
        } else {
            for entry in self
                .state
                .rows
                .entries_from(&Key::Event(SessionSeq(0), 0), false)
            {
                let Key::Event(..) = entry.key else {
                    break;
                };
                let Row::Event(stored) = &entry.value else {
                    return Err(RetirementRefusal::Corrupt);
                };
                consider(entry.key, stored)?;
            }
        }
        for key in events {
            closure.keep(key)?;
        }
        let mut keys = Vec::new();
        keys.try_reserve_exact(closure.keys.len())
            .map_err(|_| RetirementRefusal::TooLarge)?;
        keys.extend(closure.keys.iter().copied());
        Ok(RetirementFamily {
            root,
            members: closure.members,
            events: counts,
            through,
            content,
            inline,
            keys,
        })
    }
    /// Terminal, released claims from the status index, visiting at most
    /// `max_visits` index rows from `cursor` on: what a retirement driver
    /// offers to [`Core::retirement_family`], which decides eligibility
    /// beyond the claim's own state.
    pub fn retirement_candidates(
        &self,
        cursor: Option<RetirementCursor>,
        max_visits: usize,
    ) -> Result<RetirementCandidates, NativeError> {
        let terminal: Vec<ClaimStatus> = ClaimStatus::ALL
            .iter()
            .copied()
            .filter(|status| status.is_terminal())
            .collect();
        let buckets = terminal
            .iter()
            .copied()
            .skip_while(|status| cursor.is_some_and(|cursor| cursor.status != *status));
        let mut claims = Vec::new();
        let mut visited = 0usize;
        let mut after = cursor.and_then(|cursor| cursor.after);
        for status in buckets {
            let first = Key::ByStatus(status.code(), after.unwrap_or(ClaimId([0; 16])));
            let mut last = after;
            let mut exhausted = true;
            for entry in self.state.rows.entries_from(&first, after.is_some()) {
                let Key::ByStatus(code, id) = entry.key else {
                    break;
                };
                if code != status.code() {
                    break;
                }
                if visited >= max_visits {
                    exhausted = false;
                    break;
                }
                visited = visited.saturating_add(1);
                last = Some(id);
                if as_claim(self.state.rows.get(&Key::Claim(id)))
                    .is_some_and(|claim| claim.is_terminal() && claim.released())
                {
                    claims
                        .try_reserve_exact(1)
                        .map_err(|_| NativeError::Capacity("retirement candidates"))?;
                    claims.push(id);
                }
            }
            if !exhausted {
                return Ok(RetirementCandidates {
                    claims,
                    next: Some(RetirementCursor {
                        status,
                        after: last,
                    }),
                });
            }
            after = None;
        }
        Ok(RetirementCandidates { claims, next: None })
    }
    fn family_entries<'a>(
        &'a self,
        family: &'a RetirementFamily,
    ) -> impl Iterator<Item = (Key, &'a Row)> + 'a {
        family
            .keys
            .iter()
            .filter_map(|key| self.state.rows.get(key).map(|row| (*key, row)))
    }
    fn archive_frame<'m>(
        &self,
        family: &'m RetirementFamily,
        through: SessionSeq,
    ) -> record_codec::ArchiveFrame<'m> {
        record_codec::ArchiveFrame {
            ledger: self.state.ledger,
            profile: self.state.profile,
            through,
            root: family.root,
            members: &family.members,
            content: &family.content,
            inline: &family.inline,
            count: family.keys.len(),
        }
    }
    /// Measure the family's archive bundle.
    pub fn archive_family_quote(
        &self,
        family: &RetirementFamily,
        through: SessionSeq,
        limits: record_codec::EncodingLimits,
    ) -> Result<ArchiveQuote, record_codec::CodecError> {
        let mut sink = record_codec::counting_sink(limits.bytes, limits.visits);
        let hash = record_codec::archive_frame(
            &mut sink,
            self.archive_frame(family, through),
            self.family_entries(family),
            self.state.payloads(),
        )?;
        Ok(ArchiveQuote {
            bytes: sink.len(),
            visits: sink.visits_used(),
            hash,
        })
    }
    /// Write the family's archive bundle into `output` (exactly the quoted
    /// bytes), returning its digest.
    pub fn archive_family_into(
        &self,
        family: &RetirementFamily,
        through: SessionSeq,
        output: &mut [u8],
        visits: usize,
    ) -> Result<ContentHash, record_codec::CodecError> {
        let mut sink = record_codec::slice_sink(output, visits);
        let hash = record_codec::archive_frame(
            &mut sink,
            self.archive_frame(family, through),
            self.family_entries(family),
            self.state.payloads(),
        )?;
        sink.finish()?;
        Ok(hash)
    }
    /// The cold batch of the families `roots` name (doc 31 M2, A2): each derived from the
    /// committed rows and written as its archive frame at `through`, the prefix its retirement
    /// extends, after a header naming each family's root, frame digest and frame length, in
    /// `roots`' order. Every replica writes the same bytes where the record applies, so its cold
    /// stream is every replica's. Charged to `budget` in `lane` while held.
    pub fn cold_batch(
        &self,
        roots: &[ClaimId],
        through: SessionSeq,
        limits: record_codec::EncodingLimits,
        budget: &MemoryBudget,
        lane: BudgetLane,
    ) -> Result<ColdBatch, NativeError> {
        let count = u16::try_from(roots.len()).map_err(|_| NativeError::Capacity("cold batch"))?;
        let mut families = Vec::new();
        families
            .try_reserve_exact(roots.len())
            .map_err(|_| MemoryError::AllocationFailed)?;
        let header = roots
            .len()
            .checked_mul(COLD_ENTRY_BYTES)
            .and_then(|n| n.checked_add(COLD_COUNT_BYTES))
            .ok_or(NativeError::Capacity("cold batch"))?;
        let mut total = header;
        for root in roots {
            let family = self
                .retirement_family(*root)
                .map_err(|_| ContractError::InvalidManifest)?;
            let quote = self
                .archive_family_quote(&family, through, limits)
                .map_err(cold_codec)?;
            total = total
                .checked_add(quote.bytes)
                .ok_or(NativeError::Capacity("cold batch"))?;
            families.push((family, quote));
        }
        let index = size_of::<ColdEntry>()
            .checked_mul(roots.len())
            .ok_or(NativeError::Capacity("cold batch"))?;
        let charge = budget
            .reserve(
                BudgetKind::Payload,
                lane,
                total
                    .checked_add(index)
                    .ok_or(NativeError::Capacity("cold batch"))?,
            )?
            .commit();
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(total)
            .map_err(|_| MemoryError::AllocationFailed)?;
        bytes.resize(total, 0);
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(roots.len())
            .map_err(|_| MemoryError::AllocationFailed)?;
        let mut offset = header;
        for (family, quote) in &families {
            let end = offset
                .checked_add(quote.bytes)
                .ok_or(NativeError::Capacity("cold batch"))?;
            let target = bytes
                .get_mut(offset..end)
                .ok_or(NativeError::Capacity("cold batch"))?;
            let digest = self
                .archive_family_into(family, through, target, quote.visits)
                .map_err(cold_codec)?;
            if digest != quote.hash {
                return Err(NativeError::Capacity("a cold frame unlike its quote"));
            }
            entries.push(ColdEntry {
                root: family.root,
                digest,
                length: u32::try_from(quote.bytes)
                    .map_err(|_| NativeError::Capacity("cold frame"))?,
            });
            offset = end;
        }
        let mut head = Vec::new();
        head.try_reserve_exact(header)
            .map_err(|_| MemoryError::AllocationFailed)?;
        head.extend_from_slice(&count.to_le_bytes());
        for entry in &entries {
            head.extend_from_slice(&entry.root.0);
            head.extend_from_slice(&entry.digest.0);
            head.extend_from_slice(&entry.length.to_le_bytes());
        }
        bytes
            .get_mut(..header)
            .ok_or(NativeError::Capacity("cold batch"))?
            .copy_from_slice(&head);
        Ok(ColdBatch {
            bytes,
            entries,
            _charge: charge,
        })
    }
    /// Unfile every event of `family` from the event index (doc 31 M2).
    pub(super) fn unfile_family_events(&mut self, family: &RetirementFamily) {
        let NativeState {
            rows,
            events,
            ledger,
            payloads: log,
            ..
        } = &mut self.state;
        for key in &family.keys {
            let Key::Event(sequence, ordinal) = *key else {
                continue;
            };
            if !events.complete() {
                return;
            }
            match rows.get(key) {
                Some(Row::Event(stored)) => {
                    match stored.get(*ledger, crate::native::payloads::source(log)) {
                        Some(event) => events.unfile(
                            event_index::home(event),
                            event_index::Position { sequence, ordinal },
                        ),
                        None => events.drop_all(),
                    }
                }
                _ => events.drop_all(),
            }
        }
    }
    /// Retire the family (26 §4): one publication at the next native prefix
    /// that deletes every row of the family, leaves a `Retired` continuation
    /// where each member's claim row was, writes the prefix's own outcome
    /// (a `Retirement` invocation with the `Retire` operation) and the
    /// updated meta row. Returns how many rows went. The outcome it
    /// publishes is guarded as ordinary admission guards its own: one
    /// outcome per published sequence (the invariant recovery checks; a
    /// contradiction is `InvalidManifest`) and the core's outcome bound
    /// (`Capacity`), so no publication makes a state the same configuration
    /// cannot restore. The derivation refuses such a family first
    /// (`OutcomeCapacity`); the guard here is the last fence, not the check.
    pub fn retire_native_family(
        &mut self,
        family: &RetirementFamily,
        bundle: ContentHash,
        bytes: u64,
        through: SessionSeq,
    ) -> Result<usize, NativeError> {
        let mut meta = match self.state.rows.get(&Key::Meta) {
            Some(Row::Meta(meta)) => **meta,
            _ => return Err(ContractError::InvalidManifest.into()),
        };
        if u64::try_from(meta.outcomes).ok() != Some(self.native_sequence().0) {
            return Err(ContractError::InvalidManifest.into());
        }
        if meta
            .outcomes
            .saturating_sub(meta.sealed)
            .checked_add(1)
            .is_none_or(|outcomes| outcomes > self.limits.outcomes)
        {
            return Err(NativeError::Capacity("outcomes"));
        }
        let derived = self
            .retirement_family(family.root)
            .map_err(|refusal| match refusal {
                RetirementRefusal::OutcomeCapacity => NativeError::Capacity("outcomes"),
                _ => NativeError::Contract(ContractError::InvalidManifest),
            })?;
        if derived != *family
            || bundle.0 == [0; 32]
            || bytes == 0
            || through.0 == 0
            || through < family.through
        {
            return Err(ContractError::InvalidManifest.into());
        }
        let sequence = SessionSeq(
            self.native_sequence()
                .0
                .checked_add(1)
                .ok_or(NativeError::Capacity("native prefix"))?,
        );
        if through > self.native_sequence() {
            return Err(ContractError::InvalidManifest.into());
        }
        let count = family.keys.len();
        let extra = family
            .members
            .len()
            .checked_add(2)
            .ok_or(NativeError::Capacity("retirement rows"))?;
        let mut changes = Vec::new();
        changes
            .try_reserve_exact(
                count
                    .checked_add(extra)
                    .ok_or(NativeError::Capacity("retirement rows"))?,
            )
            .map_err(|_| MemoryError::AllocationFailed)?;
        for key in &family.keys {
            let Some(row) = self.state.rows.get(key) else {
                return Err(ContractError::InvalidManifest.into());
            };
            let counter = match row {
                Row::Claim(_) => Some(&mut meta.claims),
                Row::Definition(_) => Some(&mut meta.definitions),
                Row::Evaluation(_) => Some(&mut meta.evaluations),
                Row::Artifact(_) => Some(&mut meta.artifacts),
                Row::Accepted(_) | Row::MissingResult(_) | Row::DeliveryResult(_) => {
                    Some(&mut meta.results)
                }
                Row::Receipt(_) => Some(&mut meta.receipts),
                Row::Response(_) => Some(&mut meta.responses),
                Row::ResultTestament(_) => Some(&mut meta.result_testaments),
                Row::Monitor(_) => Some(&mut meta.monitors),
                Row::MonitorLink(_) => Some(&mut meta.monitor_links),
                Row::Event(_) => Some(&mut meta.events),
                Row::Outcome(outcome) => {
                    // A timer's outcome leaving with the family is sealed
                    // in its bundle (F12), its events with it.
                    meta.sealed = meta
                        .sealed
                        .checked_add(1)
                        .ok_or(NativeError::Capacity("sealed outcomes"))?;
                    meta.sealed_events = meta
                        .sealed_events
                        .checked_add(
                            usize::try_from(outcome.events)
                                .map_err(|_| NativeError::Capacity("sealed events"))?,
                        )
                        .ok_or(NativeError::Capacity("sealed events"))?;
                    None
                }
                _ => None,
            };
            if let Some(counter) = counter {
                *counter = counter
                    .checked_sub(1)
                    .ok_or(NativeError::Contract(ContractError::InvalidManifest))?;
            }
            changes.push(Change::Delete(*key));
        }
        meta.outcomes = meta
            .outcomes
            .checked_add(1)
            .ok_or(NativeError::Capacity("outcome rows"))?;
        let mut hasher = blake3::Hasher::new_derive_key("focal.native.retirement.v1");
        hasher.update(&self.state.ledger.tenant.0);
        hasher.update(&self.state.ledger.session.0);
        hasher.update(&family.root.0);
        hasher.update(&bundle.0);
        hasher.update(&through.0.to_le_bytes());
        let outcome = NativeOutcome {
            ledger: self.state.ledger,
            invocation: NativeInvocation::Retirement(family.root),
            sequence,
            logical_time: meta.logical_time,
            operation: NativeOperation::Retire,
            intent: ContentHash(*hasher.finalize().as_bytes()),
            created: 0,
            changed: u32::try_from(family.members.len())
                .map_err(|_| NativeError::Capacity("members"))?,
            definitions: 0,
            evaluations: 0,
            artifacts: 0,
            results: 0,
            receipts: 0,
            responses: 0,
            result_testaments: 0,
            events: 0,
        };
        for (member, events) in family.members.iter().zip(family.events.iter()) {
            let claim = as_claim(self.state.rows.get(&Key::Claim(*member)))
                .ok_or(NativeError::Contract(ContractError::InvalidManifest))?;
            changes.push(Change::Put(entry(
                Key::Retired(*member),
                Row::Retired(Box::new(RetiredClaim {
                    bundle,
                    bytes,
                    through,
                    binding: claim.binding(),
                    status: claim.status(),
                    retired_at: sequence,
                    events: *events,
                })),
                0,
            )));
        }
        changes.push(Change::Put(entry(Key::Meta, Row::Meta(Box::new(meta)), 0)));
        changes.push(Change::Put(entry(
            Key::Outcome(outcome.invocation),
            Row::Outcome(Box::new(OutcomeRow::stored(&outcome, self.state.ledger)?)),
            0,
        )));
        // One publication of what a family's bound allows: its rows, a
        // continuation per member, the meta and the outcome. Its events are
        // unfiled first, while their rows still say where they are filed; a
        // publication refused after that leaves the index to be built again.
        self.unfile_family_events(family);
        let bound = MAX_FAMILY_ROWS
            .saturating_add(MAX_FAMILY_MEMBERS)
            .saturating_add(2);
        if let Err(error) = self.state.rows.publish_changes(
            sequence.0,
            changes,
            focal_memory::BudgetLane::Ordinary,
            bound,
        ) {
            self.state.events.drop_all();
            return Err(error);
        }
        Ok(count)
    }
}
