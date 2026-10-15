//! Exclusive native-owner completion accounting. A grant counts an evaluation's
//! outstanding reports; it sets no bytes or record slots aside for them: each
//! count's bound keeps a completion share instead ([`CompletionBook::check_slots`]). Each
//! report is charged what it holds as it is written, on the completion lane that
//! fresh work cannot take (doc 27): nothing is prepaid for a report that may
//! never come, so an in-flight message costs the bytes it holds.
//!
//! Journals follow the owner's candidate chain. Discard must drop prepared pages
//! before rolling its journal back.
//!
//! A uniquely owned balanced index keeps stable grant slots. Inserts, updates and
//! removals touch logarithmic paths; charged buffer growth is geometric and
//! journaled for exact rollback.

use super::completion_envelope::{CompletionEnvelope, CompletionUse, ParentFacts};
use super::completion_index::{CompletionIndex, IndexGrowth};
use super::completion_schemas::SchemaSet;
use super::prepare::{add, array, within};
use super::*;
use focal_evidence::{NativeSchemaVerifier, NativeVerificationBudget};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget, OwnerId};

#[path = "completion_protection.rs"]
mod protections;
pub(super) use protections::GraphMembers;
#[path = "completion_growth.rs"]
mod growth;
#[path = "completion_updates.rs"]
mod updates;
pub(super) use updates::{JournalFunding, ReportAdvance, compare_seals};
#[path = "completion_composition.rs"]
mod composition;
pub(super) use composition::CandidateJournal;
#[path = "respondent_book.rs"]
mod respondents;
use super::respondent_state::RespondentKey;
pub(super) use respondents::respondent_journal_visits;
use respondents::{RespondentGrant, RespondentUpdate};

#[cfg(test)]
#[path = "completion_book_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "completion_book_index_tests.rs"]
mod index_tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Credit {
    binding: Binding,
    remaining_reports: u32,
    failure_available: bool,
}

#[derive(Debug)]
struct Grant {
    registration_index: usize,
    credit: Credit,
    envelope: CompletionEnvelope,
    schemas: SchemaSet,
    members: Option<GraphMembers>,
    // Cleared for the complete affected cohort before every candidate check.
    // This deduplicates temporary verification, never caches an authorization.
    growth_checked: std::cell::Cell<bool>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Totals {
    reports: usize,
    live: usize,
    graphs: usize,
}

#[derive(Debug, Clone, Copy)]
struct Update {
    key: EvaluationKey,
    before: Credit,
    after: Credit,
    // Used while sorting/folding candidate events; immutable journal logic
    // depends only on the resulting exact before/after credit.
    ordinal: u32,
    registration_index: Option<usize>,
}

/// Exact owned update-buffer charge used by the complete cohort promise.
/// Zero and one collected event use the collector's existing inline path.
pub(super) fn journal_bytes(records: usize) -> Result<usize, NativeError> {
    if records <= 1 {
        Ok(0)
    } else {
        array::<Update>(records)
    }
}

#[derive(Debug)]
enum Change {
    None,
    Begin {
        key: EvaluationKey,
        credit: Credit,
        growth: Option<IndexGrowth<Grant>>,
        protections: Option<protections::Journal>,
    },
    One(Update),
    Many {
        updates: Vec<Update>,
        _allocation: Allocation,
    },
    RespondentInstall {
        key: RespondentKey,
        credit: super::respondent_state::RespondentCredit,
        growth: Option<IndexGrowth<RespondentGrant, RespondentKey>>,
    },
    RespondentOne(RespondentUpdate),
    RespondentMany {
        updates: Vec<RespondentUpdate>,
        _allocation: Allocation,
    },
}

/// Non-cloneable ownership of exactly one speculative book change. Dropping a
/// journal alone is not rollback; the exclusive owner must commit or roll it back.
#[derive(Debug)]
pub(super) struct Journal {
    owner: Option<OwnerId>,
    before_revision: u64,
    after_revision: u64,
    change: Change,
    before: Totals,
}

impl Journal {
    pub(super) fn empty() -> Self {
        Self {
            owner: None,
            before_revision: 0,
            after_revision: 0,
            change: Change::None,
            before: Totals::default(),
        }
    }
}

/// What a report is checked against: its evaluation's contract and verification budget. The
/// report itself is charged to the state's budget, on the completion lane.
pub(super) struct ReportLoan<'a> {
    envelope: &'a CompletionEnvelope,
    verification: &'a NativeVerificationBudget,
}
impl ReportLoan<'_> {
    pub(super) fn envelope(&self) -> &CompletionEnvelope {
        self.envelope
    }
    pub(super) fn verification(&self) -> &NativeVerificationBudget {
        self.verification
    }
}

/// The owner has independently checked a due timer against the actual retained
/// evaluation. This loan requires no external reporter, schema or evidence.
pub(super) struct DeadlineLoan {
    storage: focal_memory::RangeWriteEnvelope,
}
impl DeadlineLoan {
    pub(super) fn storage(&self) -> focal_memory::RangeWriteEnvelope {
        self.storage
    }
}

#[derive(Debug)]
pub(super) struct CompletionBook {
    record_buffers: Option<record_codec::EncodingLimits>,
    entries: CompletionIndex<Grant>,
    respondents: CompletionIndex<RespondentGrant, RespondentKey>,
    protections: protections::Protections,
    parent: MemoryBudget,
    limits: NativeLimits,
    totals: Totals,
    journals: usize,
    owner: OwnerId,
    revision: u64,
}

/// Each count's bound keeps this share of itself (a sixteenth) for completing work already
/// admitted: fresh work admits within the rest ([`CompletionBook::check_slots`]).
const COMPLETION_SHARE: usize = 16;

fn sub(left: usize, right: usize) -> Result<usize, NativeError> {
    left.checked_sub(right)
        .ok_or(NativeError::Capacity("completion credit accounting"))
}
fn mul(left: usize, right: usize) -> Result<usize, NativeError> {
    left.checked_mul(right)
        .ok_or(NativeError::Capacity("completion credit accounting"))
}

/// What an evaluation's outstanding reports count against the book: their identities. Nothing is
/// set aside for the bytes or the records they may take: each report is charged what it holds as
/// it is written, and counted within the bounds' completion share, on the completion lane.
fn demand(envelope: CompletionEnvelope, credit: Credit) -> Result<Totals, NativeError> {
    if credit.remaining_reports == 0 {
        return Ok(Totals::default());
    }
    let reports = usize::try_from(credit.remaining_reports)
        .map_err(|_| NativeError::Capacity("completion report count"))?;
    Ok(Totals {
        reports,
        live: 1,
        graphs: usize::from(envelope.has_graph()),
    })
}

impl Totals {
    fn replace(self, old: Self, new: Self) -> Result<Self, NativeError> {
        Ok(Self {
            reports: add(sub(self.reports, old.reports)?, new.reports)?,
            live: add(sub(self.live, old.live)?, new.live)?,
            graphs: add(sub(self.graphs, old.graphs)?, new.graphs)?,
        })
    }
}

impl CompletionBook {
    pub(super) fn new(source: &MemoryBudget, limits: NativeLimits) -> Result<Self, NativeError> {
        let owner = OwnerId::new()?;
        Ok(Self {
            record_buffers: None,
            entries: CompletionIndex::new(),
            respondents: CompletionIndex::new(),
            protections: protections::Protections::new(),
            parent: source.clone(),
            limits,
            totals: Totals::default(),
            journals: 0,
            owner,
            revision: 0,
        })
    }

    pub(super) fn with_record_buffers(mut self, limits: record_codec::EncodingLimits) -> Self {
        self.record_buffers = Some(limits);
        self
    }
    pub(super) fn check_health(&self) -> Result<(), NativeError> {
        self.entries.check_health()?;
        self.respondents.check_health()?;
        self.protections.check_health()
    }
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }
    #[cfg(test)]
    pub(super) fn remaining_reports(&self, key: EvaluationKey) -> Option<u32> {
        self.entries
            .get(key)
            .map(|entry| entry.credit.remaining_reports)
    }
    #[cfg(test)]
    pub(super) fn envelope_for_test(&self, key: EvaluationKey) -> Option<&CompletionEnvelope> {
        self.entries.get(key).map(|entry| &entry.envelope)
    }

    fn grant(&self, key: EvaluationKey) -> Result<&Grant, NativeError> {
        self.entries
            .get(key)
            .ok_or(ContractError::StaleEvaluation.into())
    }

    #[cfg(test)]
    pub(super) fn install_begin(
        &mut self,
        key: EvaluationKey,
        binding: Binding,
        envelope: CompletionEnvelope,
        schemas: SchemaSet,
        registration_index: usize,
    ) -> Result<Journal, NativeError> {
        self.install(
            key,
            binding,
            envelope.reports(),
            envelope,
            schemas,
            registration_index,
            None,
        )
    }

    pub(super) fn install_begin_with_members(
        &mut self,
        key: EvaluationKey,
        binding: Binding,
        envelope: CompletionEnvelope,
        schemas: SchemaSet,
        registration_index: usize,
        members: Option<GraphMembers>,
    ) -> Result<Journal, NativeError> {
        self.install(
            key,
            binding,
            envelope.reports(),
            envelope,
            schemas,
            registration_index,
            members,
        )
    }

    /// Reconstruction is driven by the owner's complete actual registered-row
    /// scan. It includes begun unfenced Admission and Increment chains after receipt or parent
    /// failure. The owner derives remaining attempts from the exact definition.
    #[cfg(test)]
    pub(super) fn install_recovered(
        &mut self,
        key: EvaluationKey,
        binding: Binding,
        remaining_reports: u32,
        envelope: CompletionEnvelope,
        schemas: SchemaSet,
        registration_index: usize,
    ) -> Result<(), NativeError> {
        self.install_recovered_with_members(
            key,
            binding,
            remaining_reports,
            envelope,
            schemas,
            registration_index,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)] // Exact owner reconstruction contract.
    pub(super) fn install_recovered_with_members(
        &mut self,
        key: EvaluationKey,
        binding: Binding,
        remaining_reports: u32,
        envelope: CompletionEnvelope,
        schemas: SchemaSet,
        registration_index: usize,
        members: Option<GraphMembers>,
    ) -> Result<(), NativeError> {
        if self.journals != 0 {
            return Err(ContractError::InvalidTransition.into());
        }
        let journal = self.install(
            key,
            binding,
            remaining_reports,
            envelope,
            schemas,
            registration_index,
            members,
        )?;
        self.commit(journal)
    }

    #[allow(clippy::too_many_arguments)] // One grant and its checked protected membership.
    fn install(
        &mut self,
        key: EvaluationKey,
        binding: Binding,
        remaining_reports: u32,
        envelope: CompletionEnvelope,
        schemas: SchemaSet,
        registration_index: usize,
        members: Option<GraphMembers>,
    ) -> Result<Journal, NativeError> {
        let envelope = match self.record_buffers {
            Some(limits) => envelope.with_record_buffers(limits)?,
            None => envelope,
        };
        if !envelope.supports_target(key.target)
            || envelope.has_graph() != members.is_some()
            || key.validation.0 != binding.object.0
            || remaining_reports == 0
            || remaining_reports > envelope.reports()
            || self.entries.get(key).is_some()
        {
            return Err(ContractError::InvalidTransition.into());
        }
        let credit = Credit {
            binding,
            remaining_reports,
            failure_available: envelope
                .report_storage(CompletionUse::AdmissionFailure)
                .is_ok(),
        };
        let before = self.totals;
        let totals = before.replace(Totals::default(), demand(envelope, credit)?)?;
        let journals = add(self.journals, 1)?;
        let revision = self.next_revision()?;
        let protection_limit = if members.is_some() {
            mul(self.limits.evaluations, self.limits.plan_nodes)?
        } else {
            0
        };
        let growth = self.entries.grow(&self.parent, self.limits.evaluations)?;
        let protections = if let Some(members) = &members {
            match self
                .protections
                .install(&self.parent, key, members, protection_limit)
            {
                Ok(journal) => Some(journal),
                Err(error) => {
                    if let Some(growth) = growth {
                        self.entries.restore_growth(growth)?;
                    }
                    return Err(error);
                }
            }
        } else {
            None
        };
        // Growth precedes tree mutation; insert allocates nothing.
        if let Err(error) = self.entries.insert(
            key,
            Grant {
                registration_index,
                credit,
                envelope,
                schemas,
                members,
                growth_checked: std::cell::Cell::new(false),
            },
        ) {
            if let Some(protections) = protections {
                self.protections.rollback(protections)?;
            }
            if let Some(growth) = growth {
                self.entries.restore_growth(growth)?;
            }
            return Err(error);
        }
        self.totals = totals;
        self.journals = journals;
        let before_revision = self.revision;
        self.revision = revision;
        Ok(Journal {
            owner: Some(self.owner),
            before_revision,
            after_revision: revision,
            change: Change::Begin {
                key,
                credit,
                growth,
                protections,
            },
            before,
        })
    }

    #[allow(clippy::too_many_arguments)] // One checked owner frame, not participant configuration.
    pub(super) fn report_contract<'a>(
        &'a self,
        key: EvaluationKey,
        binding: Binding,
        parent: &ClaimState,
        registry: &RegistrationSet,
        input: &NativeArtifactInput,
        schema: ContentHash,
        verifier: &impl NativeSchemaVerifier,
    ) -> Result<ReportLoan<'a>, NativeError> {
        self.report_contract_with(
            key,
            binding,
            parent,
            registry,
            schema,
            verifier,
            |envelope| envelope.check_descriptor(input),
        )
    }

    #[allow(clippy::too_many_arguments)] // Same exact held contract, before artifact allocation.
    pub(super) fn report_contract_view<'a>(
        &'a self,
        key: EvaluationKey,
        binding: Binding,
        parent: &ClaimState,
        registry: &RegistrationSet,
        descriptor: &impl super::report_artifact::ArtifactView,
        schema: ContentHash,
        verifier: &impl NativeSchemaVerifier,
    ) -> Result<ReportLoan<'a>, NativeError> {
        self.report_contract_with(
            key,
            binding,
            parent,
            registry,
            schema,
            verifier,
            |envelope| envelope.check_descriptor_view(descriptor),
        )
    }

    #[allow(clippy::too_many_arguments)] // One allocation-free descriptor checker within the held frame.
    fn report_contract_with<'a>(
        &'a self,
        key: EvaluationKey,
        binding: Binding,
        parent: &ClaimState,
        registry: &RegistrationSet,
        schema: ContentHash,
        verifier: &impl NativeSchemaVerifier,
        check_descriptor: impl FnOnce(&CompletionEnvelope) -> Result<(), NativeError>,
    ) -> Result<ReportLoan<'a>, NativeError> {
        self.check_health()?;
        let grant = self.grant(key)?;
        grant.credit.binding.check(&binding)?;
        if grant.credit.remaining_reports == 0 {
            return Err(ContractError::InvalidTransition.into());
        }
        grant.envelope.check_parent(parent, registry)?;
        if registry
            .rows()
            .get(grant.registration_index)
            .map(|row| transactions::key_for_registered(key.claim, *row))
            != Some(key)
        {
            return Err(ContractError::InvalidTarget.into());
        }
        check_descriptor(&grant.envelope)?;
        let verification = grant.schemas.budget_for(schema, verifier)?;
        Ok(ReportLoan {
            envelope: &grant.envelope,
            verification,
        })
    }

    pub(super) fn deadline_contract(
        &self,
        key: EvaluationKey,
        binding: Binding,
    ) -> Result<DeadlineLoan, NativeError> {
        self.check_health()?;
        let grant = self.grant(key)?;
        grant.credit.binding.check(&binding)?;
        if grant.credit.remaining_reports == 0 {
            return Err(ContractError::InvalidTransition.into());
        }
        Ok(DeadlineLoan {
            storage: grant.envelope.deadline_storage(self.limits)?,
        })
    }

    #[cfg(test)]
    pub(super) fn advance(
        &mut self,
        key: EvaluationKey,
        before: Binding,
        after: Binding,
        terminal: bool,
        usage: CompletionUse,
    ) -> Result<Journal, NativeError> {
        let grant = self.grant(key)?;
        grant.credit.binding.check(&before)?;
        Binding {
            revision: before.revision,
            ..after
        }
        .check(&before)?;
        if after.revision <= before.revision
            || grant.credit.remaining_reports == 0
            || (usage == CompletionUse::AdmissionFailure && !grant.credit.failure_available)
        {
            return Err(ContractError::InvalidTransition.into());
        }
        let remaining = grant
            .credit
            .remaining_reports
            .checked_sub(1)
            .ok_or(ContractError::InvalidTransition)?;
        if !terminal && remaining == 0 {
            return Err(ContractError::InvalidTransition.into());
        }
        let next = Credit {
            binding: after,
            remaining_reports: if terminal { 0 } else { remaining },
            failure_available: grant.credit.failure_available
                && usage != CompletionUse::AdmissionFailure,
        };
        let update = Update {
            key,
            before: grant.credit,
            after: next,
            ordinal: 0,
            registration_index: Some(grant.registration_index),
        };
        self.apply_single_update(update)
    }

    fn apply_single_update(&mut self, update: Update) -> Result<Journal, NativeError> {
        let key = update.key;
        let grant = self.grant(key)?;
        if grant.credit != update.before {
            return Err(ContractError::StaleEvaluation.into());
        }
        let next = update.after;
        let totals = self.totals.replace(
            demand(grant.envelope, grant.credit)?,
            demand(grant.envelope, next)?,
        )?;
        let journals = add(self.journals, 1)?;
        let revision = self.next_revision()?;
        let before = self.totals;
        self.entries.update(key, |grant| grant.credit = next)?;
        self.totals = totals;
        self.check_health()?;
        self.journals = journals;
        let before_revision = self.revision;
        self.revision = revision;
        Ok(Journal {
            owner: Some(self.owner),
            before_revision,
            after_revision: revision,
            change: Change::One(update),
            before,
        })
    }

    pub(super) fn event(
        prepared: &NativePrepared,
        ordinal: u32,
        payloads: Option<&dyn crate::native::payloads::PayloadRead>,
    ) -> Result<NativeEvent, NativeError> {
        match prepared
            .fragments
            .get(&Key::Event(prepared.outcome.sequence, ordinal))
        {
            // A candidate's events are spilled as they are built (doc 31 M3).
            Some(Row::Event(event)) => event
                .get(prepared.outcome.ledger, payloads)
                .ok_or(ContractError::InvalidCut.into()),
            _ => Err(ContractError::InvalidCut.into()),
        }
    }

    /// The original Admission failure precedes graph revisions and cohort
    /// seals. Resolve that publication instead of inferring it from a final row.
    pub(super) fn admission_failure_event(
        prepared: &NativePrepared,
        payloads: Option<&dyn crate::native::payloads::PayloadRead>,
    ) -> Result<Option<NativeClaimEvent>, NativeError> {
        if prepared.outcome.operation != NativeOperation::ReportAdmission
            || prepared.outcome.events <= 3
        {
            return Ok(None);
        }
        let publication = Self::event(prepared, 3, payloads)?;
        let NativeFact::Claim(event) = publication.fact else {
            return Err(ContractError::InvalidCut.into());
        };
        let parent = prepared
            .written_claim(ClaimId(event.after.object.0))
            .ok_or(ContractError::InvalidTarget)?;
        if publication.sequence != prepared.outcome.sequence
            || publication.ordinal != 3
            || publication.invocation != prepared.outcome.invocation
            || event.kind != NativeEventKind::PostFailed
            || !matches!(parent.terminal_cut(),
                Some(focal_model::lifecycle::claim::ClaimTerminalCut::Required(cut))
                if cut.sequence() == prepared.outcome.sequence)
        {
            return Err(ContractError::InvalidCut.into());
        }
        Ok(Some(event))
    }

    /// Protect every active grant on each parent actually changed by this
    /// candidate; registered definitions supply the complete lookup keys.
    pub(super) fn check_parents(
        &self,
        view: &View<'_>,
        prepared: &NativePrepared,
    ) -> Result<(), NativeError> {
        self.check_health()?;
        if view.tail().is_none_or(|tail| !std::ptr::eq(tail, prepared)) {
            return Err(MemoryError::WrongRange.into());
        }
        let mut previous = None;
        for ordinal in 0..prepared.outcome.events {
            let after = match Self::event(prepared, ordinal, view.payloads())?.fact {
                NativeFact::Claim(event) => event.after,
                NativeFact::Registrations { claim } => claim,
                NativeFact::Evaluation { key, .. } => {
                    let state = prepared
                        .written_evaluation(key)
                        .ok_or(ContractError::InvalidTarget)?;
                    // Complete source cohorts were checked before accepting a
                    // responsibility. Every later evaluation mutation has a
                    // fact, so preserving Ready headroom needs only its final
                    // row, including authority-only deadline fences.
                    if !state.has_begun()
                        && !state.state().is_terminal()
                        && state.sealed().is_none()
                    {
                        state.binding().next()?;
                    }
                    continue;
                }
                _ => continue,
            };
            let id = ClaimId(after.object.0);
            let Some(Row::Claim(owned)) = prepared.fragments.get(&Key::Claim(id)) else {
                return Err(ContractError::InvalidTarget.into());
            };
            let parent = prepared
                .written_claim(id)
                .ok_or(ContractError::InvalidTarget)?;
            // One batch may append several child facts for the same parent.
            // Only its final revision matches the actual retained row; every
            // changed claim emits that final event before candidate creation.
            if after != parent.binding() {
                continue;
            }
            if previous == Some(id) {
                continue;
            }
            previous = Some(id);
            // Seek this parent's own grant interval. Iterating only supplied
            // registration rows would silently miss a dropped whole cohort.
            let first = EvaluationKey {
                claim: id,
                validation: ValidationId::from_u128(0),
                target: EvaluationTarget::Admission,
                generation: 0,
            };
            let mut facts = None;
            for (key, grant) in self
                .entries
                .iter_from(first)
                .take_while(|(key, _)| key.claim == id)
            {
                if grant.credit.remaining_reports == 0 {
                    continue;
                }
                let registry = owned.registrations().ok_or(ContractError::InvalidTarget)?;
                if facts.is_none() {
                    facts = Some(ParentFacts::new(parent, registry)?);
                }
                grant
                    .envelope
                    .check_parent_facts(facts.as_ref().ok_or(ContractError::InvalidTarget)?)?;
                if registry
                    .rows()
                    .get(grant.registration_index)
                    .map(|row| transactions::key_for_registered(id, *row))
                    != Some(key)
                {
                    return Err(ContractError::InvalidTarget.into());
                }
            }
        }
        self.check_health()
    }

    /// Check the candidate's actual effective counts after its credit journal is
    /// applied, on its lane: each count's bound keeps a completion share (doc 27),
    /// so fresh work admits within the bound less its share and work already
    /// admitted completes within the rest. Nothing is set aside for a grant's
    /// future reports. Resident outcomes have no share: a seal under pressure is
    /// what makes room for them, for fresh work and completion alike. One more
    /// outcome and sequence stay for an authority control.
    pub(super) fn check_slots(
        &self,
        meta: Meta,
        sequence: SessionSeq,
        rows: usize,
        lane: BudgetLane,
    ) -> Result<(), NativeError> {
        self.check_health()?;
        let ceiling = |limit: usize| match lane {
            BudgetLane::Ordinary => {
                limit.saturating_sub(limit.checked_div(COMPLETION_SHARE).unwrap_or(0))
            }
            BudgetLane::Completion | BudgetLane::Committed => limit,
        };
        within(meta.claims, ceiling(self.limits.claims))?;
        within(meta.definitions, ceiling(self.limits.definitions))?;
        within(meta.evaluations, ceiling(self.limits.evaluations))?;
        within(meta.artifacts, ceiling(self.limits.artifacts))?;
        within(meta.results, ceiling(self.limits.results))?;
        within(meta.responses, ceiling(self.limits.responses))?;
        within(meta.receipts, ceiling(self.limits.receipts))?;
        // Exactly one claimant result testament may be generated per claim.
        // Its native admission path shares the configured claim-count ceiling.
        within(meta.result_testaments, ceiling(self.limits.claims))?;
        within(meta.monitors, ceiling(self.limits.monitors))?;
        within(meta.monitor_links, ceiling(self.limits.monitor_links))?;
        within(meta.events, ceiling(self.limits.events))?;
        let control = usize::from(self.totals.live != 0);
        // Resident outcomes (F12): the lifetime count less what left into
        // seals and retirement bundles.
        within(
            add(meta.outcomes.saturating_sub(meta.sealed), control)?,
            self.limits.outcomes,
        )?;
        sequence
            .0
            .checked_add(u64::from(control != 0))
            .ok_or(NativeError::Capacity("completion sequence margin"))?;
        add(rows, control)?;
        Ok(())
    }

    /// Candidate identities have their own finite counter. Reserve one ticket
    /// for each promised report and one control; discarded tickets are never
    /// reused. Repeated discard can exhaust an incarnation and require checked
    /// reconstruction under a fresh owner identity.
    pub(super) fn check_serial(&self, serial: u64) -> Result<(), NativeError> {
        let reports = u64::try_from(self.totals.reports)
            .map_err(|_| NativeError::Capacity("completion candidate count"))?;
        serial
            .checked_add(reports)
            .and_then(|value| value.checked_add(u64::from(self.totals.live != 0)))
            .ok_or(NativeError::Capacity("completion candidate margin"))?;
        self.revision
            .checked_add(
                reports
                    .checked_mul(2)
                    .ok_or(NativeError::Capacity("completion journal margin"))?,
            )
            .and_then(|value| value.checked_add(if self.totals.live != 0 { 4 } else { 0 }))
            .ok_or(NativeError::Capacity("completion journal margin"))?;
        Ok(())
    }

    fn check_update(&self, update: Update) -> Result<(), NativeError> {
        if self.grant(update.key)?.credit != update.after {
            return Err(ContractError::StaleEvaluation.into());
        }
        Ok(())
    }

    pub(super) fn rollback(&mut self, journal: Journal) -> Result<(), NativeError> {
        if matches!(journal.change, Change::None) {
            return Ok(());
        }
        self.check_journal(&journal)?;
        if self.revision != journal.after_revision {
            return Err(ContractError::InvalidTransition.into());
        }
        let journals = sub(self.journals, 1)?;
        match &journal.change {
            Change::Begin {
                key,
                credit,
                growth,
                ..
            } => {
                if self.grant(*key)?.credit != *credit {
                    return Err(ContractError::StaleEvaluation.into());
                }
                if let Some(growth) = growth {
                    self.entries.check_remove_restore_growth(growth, *key)?;
                }
            }
            Change::One(update) => self.check_update(*update)?,
            Change::Many { updates, .. } => {
                for update in updates {
                    self.check_update(*update)?;
                }
            }
            Change::RespondentInstall {
                key,
                credit,
                growth,
                ..
            } => {
                self.check_respondent_install(*key, *credit, growth.as_ref())?;
            }
            Change::RespondentOne(update) => self.check_respondent_update(*update)?,
            Change::RespondentMany { updates, .. } => {
                for update in updates {
                    self.check_respondent_update(*update)?;
                }
            }
            Change::None => {}
        }
        match journal.change {
            Change::Begin {
                key,
                growth,
                protections,
                ..
            } => {
                if let Some(protections) = protections {
                    self.protections.rollback(protections)?;
                }
                drop(self.entries.remove(key)?);
                if let Some(growth) = growth {
                    self.entries.restore_growth(growth)?;
                }
            }
            Change::One(update) => self.restore_credit(update)?,
            Change::Many {
                updates,
                _allocation,
            } => {
                for update in updates {
                    self.restore_credit(update)?;
                }
                drop(_allocation);
            }
            Change::RespondentInstall { key, growth, .. } => {
                self.respondents.remove(key)?;
                if let Some(growth) = growth {
                    self.respondents.restore_growth(growth)?;
                }
            }
            Change::RespondentOne(update) => self.restore_respondent(update)?,
            Change::RespondentMany {
                updates,
                _allocation,
            } => {
                for update in updates {
                    self.restore_respondent(update)?;
                }
                drop(_allocation);
            }
            Change::None => {}
        }
        self.totals = journal.before;
        self.journals = journals;
        self.revision = journal.before_revision;
        Ok(())
    }

    fn restore_credit(&mut self, update: Update) -> Result<(), NativeError> {
        self.grant(update.key)?;
        self.entries
            .update(update.key, |grant| grant.credit = update.before)
    }

    fn remove_grant(&mut self, key: EvaluationKey) -> Result<(), NativeError> {
        let grant = self
            .entries
            .get(key)
            .ok_or(ContractError::StaleEvaluation)?;
        if let Some(members) = &grant.members {
            self.protections.remove(key, members)?;
        }
        drop(self.entries.remove(key)?);
        Ok(())
    }

    pub(super) fn commit(&mut self, journal: Journal) -> Result<(), NativeError> {
        if matches!(journal.change, Change::None) {
            return Ok(());
        }
        self.check_journal(&journal)?;
        let journals = sub(self.journals, 1)?;
        match &journal.change {
            Change::One(update) if update.after.remaining_reports == 0 => {
                self.check_update(*update)?
            }
            Change::Many { updates, .. } => {
                for update in updates {
                    if update.after.remaining_reports == 0 {
                        self.check_update(*update)?;
                    }
                }
            }
            Change::RespondentOne(update) if update.after.actions()? == 0 => {
                self.check_respondent_update(*update)?;
            }
            Change::RespondentMany { updates, .. } => {
                for update in updates {
                    if update.after.actions()? == 0 {
                        self.check_respondent_update(*update)?;
                    }
                }
            }
            _ => {}
        }
        match &journal.change {
            Change::One(update) if update.after.remaining_reports == 0 => {
                self.remove_grant(update.key)?;
            }
            Change::Many { updates, .. } => {
                for update in updates {
                    if update.after.remaining_reports == 0 {
                        self.remove_grant(update.key)?;
                    }
                }
            }
            Change::RespondentOne(update) if update.after.actions()? == 0 => {
                self.respondents.remove(update.key)?;
            }
            Change::RespondentMany { updates, .. } => {
                for update in updates {
                    if update.after.actions()? == 0 {
                        self.respondents.remove(update.key)?;
                    }
                }
            }
            _ => {}
        }
        self.journals = journals;
        // Dropping journal releases only its old empty buffer / retirement
        // bookkeeping.
        Ok(())
    }

    fn check_journal(&self, journal: &Journal) -> Result<(), NativeError> {
        if journal.owner != Some(self.owner) {
            return Err(ContractError::InvalidTransition.into());
        }
        Ok(())
    }

    fn next_revision(&self) -> Result<u64, NativeError> {
        self.revision
            .checked_add(1)
            .ok_or(NativeError::Capacity("completion journal revision"))
    }
}
