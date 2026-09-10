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
use focal_memory::{Change, Entry};
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
    /// The committed rows contradict themselves.
    Corrupt,
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
            if matches!(entry.key, Key::Outcome(_) | Key::CreationResult(_)) {
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
    /// The family rooted at `root` and every row it takes to the archive,
    /// or why it cannot leave yet. Deterministic over the committed rows.
    pub fn retirement_family(&self, root: ClaimId) -> Result<RetirementFamily, RetirementRefusal> {
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
        for member in closure.members.clone() {
            let (claim, content) = match self.state.rows.get(&Key::Claim(member)) {
                Some(Row::Claim(owned)) => {
                    let claim = owned.claim().ok_or(RetirementRefusal::Corrupt)?;
                    let content = match self.state.rows.get(&Key::ClaimContent(member)) {
                        Some(Row::ClaimContent(content)) => content.get(),
                        _ => None,
                    };
                    (claim, content)
                }
                _ => return Err(RetirementRefusal::Corrupt),
            };
            closure.index_keys(|sink| {
                index_rows::claim(None, claim, content, &index_rows::NeverConsumed, sink)
            })?;
            if let Some(content) = content {
                closure
                    .keep_existing(Key::ClaimIdentity(content.schema(), content.content_hash()))?;
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
        // they describe.
        let mut events = Vec::new();
        let mut through = SessionSeq(0);
        let mut counts = Vec::new();
        counts
            .try_reserve_exact(closure.members.len())
            .map_err(|_| RetirementRefusal::TooLarge)?;
        counts.extend(closure.members.iter().map(|_| 0u32));
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
            let event = stored
                .get()
                .ok_or(RetirementRefusal::Corrupt)?
                .expand(self.state.ledger);
            let object = record_codec::event_object(event);
            if closure.keys.contains(&object) {
                if events.len() >= MAX_FAMILY_ROWS {
                    return Err(RetirementRefusal::TooLarge);
                }
                events.push(entry.key);
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
        )?;
        sink.finish()?;
        Ok(hash)
    }
    /// Retire the family (26 §4): one publication at the next native prefix
    /// that deletes every row of the family, leaves a `Retired` continuation
    /// where each member's claim row was, writes the prefix's own outcome
    /// (a `Retirement` invocation with the `Retire` operation) and the
    /// updated meta row. Returns how many rows went.
    pub fn retire_native_family(
        &mut self,
        family: &RetirementFamily,
        bundle: ContentHash,
        bytes: u64,
        through: SessionSeq,
    ) -> Result<usize, NativeError> {
        let derived = self
            .retirement_family(family.root)
            .map_err(|_| NativeError::Contract(ContractError::InvalidManifest))?;
        if derived != *family
            || bundle.0 == [0; 32]
            || bytes == 0
            || through.0 == 0
            || through < family.through
        {
            return Err(ContractError::InvalidManifest.into());
        }
        let mut meta = match self.state.rows.get(&Key::Meta) {
            Some(Row::Meta(meta)) => *meta,
            _ => return Err(ContractError::InvalidManifest.into()),
        };
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
            changes.push(Change::Put(Entry::new(
                Key::Retired(*member),
                Row::Retired(RetiredClaim {
                    bundle,
                    bytes,
                    through,
                    binding: claim.binding(),
                    status: claim.status(),
                    retired_at: sequence,
                    events: *events,
                }),
                0,
            )));
        }
        changes.push(Change::Put(Entry::new(Key::Meta, Row::Meta(meta), 0)));
        changes.push(Change::Put(Entry::new(
            Key::Outcome(outcome.invocation),
            Row::Outcome(outcome),
            0,
        )));
        self.state
            .rows
            .publish_changes(sequence.0, changes, focal_memory::BudgetLane::Ordinary)?;
        Ok(count)
    }
}
