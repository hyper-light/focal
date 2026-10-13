//! Retirement in the owner's pipeline (doc 31 M2). The owner prepares the retirement of a batch
//! of families as one candidate at the sequence its chain has reached, ordered with every write
//! as a timer's deadline is: no exact prefix to wait for, no pipeline to drain, no admission
//! held, no owner rebuilt. Each family is derived from the committed rows (an eligible family's
//! rows cannot change, since anything that refers to it makes it ineligible) and is deferred
//! where a pending candidate changes a row its derivation reads, so the family the chain would
//! derive is the family the committed rows derive. Every replica derives the same families from
//! the same rows at the record's place and checks them against the batch's digest; a family a
//! replica cannot derive alike is a divergence, never a retirement skipped.
//!
//! A family is offered with the archive bundle that holds its rows, sealed under custody before
//! it is offered (26 §4), and with the digest of the family that bundle was made of: a family
//! derived at the batch's place that is not that family is refused, so a bundle never stands
//! for rows it does not hold.
use super::pending_index::Chain;
use super::retirement::{MAX_FAMILY_ROWS, RetirementFamily, RetirementRefusal};
use super::*;
use focal_memory::{BudgetKind, BudgetLane, Change};

/// The most families one retirement takes: a batch's derivation, its record and its
/// publication stay bounded, and a burst of completions retires in batches of this many.
pub const MAX_RETIRE_FAMILIES: usize = 64;

/// A family offered for retirement: its root, the digest of the family its bundle holds
/// ([`family_digest`]), and the bundle, the content root of an object of the ledger's tenant
/// domain under the checkpoint class, with its length.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetireOffer {
    pub root: ClaimId,
    pub family: ContentHash,
    pub bundle: ContentHash,
    pub bytes: u64,
}

/// A family's place in a retirement record: its root and the bundle that holds its rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetiredBundle {
    pub root: ClaimId,
    pub bundle: ContentHash,
    pub bytes: u64,
}

/// The most rows one retirement's candidate changes: one family's bound, its members'
/// continuations, the meta and the outcome. A batch of several families stays within the
/// store's bound on a batch; a family past that retires alone.
pub(super) const MAX_RETIRE_CHANGES: usize = MAX_FAMILY_ROWS
    .saturating_add(super::retirement::MAX_FAMILY_MEMBERS)
    .saturating_add(2);

/// A family a batch retires, and the bundle that holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Retiring {
    pub family: RetirementFamily,
    pub bundle: ContentHash,
    pub bytes: u64,
}

/// What a batch made of the families it was offered.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RetireBatch {
    /// The families that retire, in the order they were offered.
    pub families: Vec<Retiring>,
    /// Roots whose family a pending candidate changes, or past the batch's bound: offered
    /// again once the candidate commits.
    pub deferred: Vec<ClaimId>,
    /// Roots that cannot retire now, and why.
    pub refused: Vec<(ClaimId, RetireRefusal)>,
}

/// Why an offered family does not retire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetireRefusal {
    /// Its family cannot leave the core now.
    Family(RetirementRefusal),
    /// The family derived here is not the family its bundle holds.
    Bundle,
}

impl RetireBatch {
    /// Rows the batch takes from the core.
    pub fn rows(&self) -> usize {
        self.families.iter().fold(0usize, |total, retiring| {
            total.saturating_add(retiring.family.rows())
        })
    }
    /// The roots that retire.
    pub fn roots(&self) -> impl Iterator<Item = ClaimId> + '_ {
        self.families.iter().map(|retiring| retiring.family.root)
    }
}

/// What a family is, as every replica derives it: its root, members and every row it takes,
/// in order, each key as a record writes it.
pub fn family_digest(
    ledger: LedgerId,
    family: &RetirementFamily,
) -> Result<ContentHash, NativeError> {
    let mut hasher = blake3::Hasher::new_derive_key("focal.native.retirement.family.v1");
    hasher.update(&ledger.tenant.0);
    hasher.update(&ledger.session.0);
    feed_family(&mut hasher, family)?;
    Ok(ContentHash(*hasher.finalize().as_bytes()))
}

fn feed_family(hasher: &mut blake3::Hasher, family: &RetirementFamily) -> Result<(), NativeError> {
    let length = |len: usize| u64::try_from(len).map_err(|_| NativeError::Capacity("family"));
    hasher.update(&family.root.0);
    hasher.update(&length(family.members.len())?.to_le_bytes());
    for member in &family.members {
        hasher.update(&member.0);
    }
    hasher.update(&length(family.keys.len())?.to_le_bytes());
    for key in &family.keys {
        record_codec::hash_key(hasher, *key)
            .map_err(|_| NativeError::Capacity("a retiring key past what a record writes"))?;
    }
    Ok(())
}

/// What a batch is: every family it retires, as [`family_digest`] feeds it, and its bundle.
/// The batch's outcome carries it as its intent, and every replica checks it.
pub(super) fn batch_digest(
    ledger: LedgerId,
    families: &[Retiring],
) -> Result<ContentHash, NativeError> {
    let mut hasher = blake3::Hasher::new_derive_key("focal.native.retirement.batch.v1");
    hasher.update(&ledger.tenant.0);
    hasher.update(&ledger.session.0);
    for retiring in families {
        feed_family(&mut hasher, &retiring.family)?;
        hasher.update(&retiring.bundle.0);
        hasher.update(&retiring.bytes.to_le_bytes());
    }
    Ok(ContentHash(*hasher.finalize().as_bytes()))
}

/// The least key under `affinity`: where a scan of what lies under it starts.
fn first_under(affinity: [u8; 16]) -> Key {
    Key::IncomingHead(ClaimId(affinity))
}

/// The affinity `key` sits under when it is its own object's: a claim's, or the artifact,
/// testament, declaration or monitor it names. Rows under shared affinities (the control
/// rows, index buckets, identities) are checked one by one instead.
fn own_home(key: &Key) -> Option<[u8; 16]> {
    match key {
        Key::Claim(claim) => Some(claim.0),
        Key::Artifact(artifact) | Key::Work(artifact) | Key::Diagnostic(artifact) => {
            Some(artifact.0)
        }
        Key::Response(testament) | Key::ResultTestament(testament) => Some(testament.0),
        Key::Definition(definition) => Some(definition.0),
        Key::Monitor(monitor) => Some(monitor.0),
        _ => None,
    }
}

/// Whether a pending candidate on `chain` changes a row `family`'s derivation read: one under
/// the affinity of a member or of an object it owns (where every row that refers to one of them
/// lies), or one of its own rows.
fn touched(chain: &Chain<'_>, family: &RetirementFamily) -> bool {
    let homes = family
        .members
        .iter()
        .map(|member| member.0)
        .chain(family.keys.iter().filter_map(own_home));
    for home in homes {
        if chain
            .changes_from(&first_under(home), false)
            .next()
            .is_some_and(|(key, _)| layout::affinity(key) == home)
        {
            return true;
        }
    }
    family.keys.iter().any(|key| chain.get(key).is_some())
}

impl Core<NativeState> {
    /// Plan the retirement of the families `offers` name on `chain` (the committed rows where
    /// none): each derived from the committed rows and refused where it cannot leave or is not
    /// the family its bundle holds, deferred where a pending candidate changes a row its
    /// derivation read. The batch stops at [`MAX_RETIRE_FAMILIES`] families and at the store's
    /// bound on a batch's changes, past which only a first family may go, alone; the offers
    /// past either are deferred.
    pub(super) fn plan_retirement_batch(
        &self,
        offers: &[RetireOffer],
        chain: Option<Chain<'_>>,
    ) -> Result<RetireBatch, NativeError> {
        if offers.len() > MAX_RETIRE_FAMILIES {
            return Err(NativeError::Capacity("retirement offers"));
        }
        let mut batch = RetireBatch::default();
        // Each offer lands in one of the three, once: within these, no push reallocates.
        for vector_len in [
            batch.families.try_reserve_exact(offers.len()),
            batch.deferred.try_reserve_exact(offers.len()),
            batch.refused.try_reserve_exact(offers.len()),
        ] {
            vector_len.map_err(|_| MemoryError::AllocationFailed)?;
        }
        let bound = self.limits.range.max_batch_entries;
        let mut rows = 2usize;
        for offer in offers {
            if batch
                .families
                .iter()
                .any(|retiring| retiring.family.members.contains(&offer.root))
                || batch.deferred.contains(&offer.root)
                || batch.refused.iter().any(|(root, _)| *root == offer.root)
            {
                // Offered twice, or a member of a family already taken.
                continue;
            }
            let family = match self.retirement_family(offer.root) {
                Ok(family) => family,
                Err(refusal) => {
                    batch
                        .refused
                        .push((offer.root, RetireRefusal::Family(refusal)));
                    continue;
                }
            };
            if family_digest(self.state.ledger, &family)? != offer.family
                || offer.bundle.0 == [0; 32]
                || offer.bytes == 0
            {
                batch.refused.push((offer.root, RetireRefusal::Bundle));
                continue;
            }
            let taken = rows
                .checked_add(family.rows())
                .and_then(|taken| taken.checked_add(family.members.len()));
            let fits = taken.is_some_and(|taken| {
                taken <= bound || (batch.families.is_empty() && taken <= MAX_RETIRE_CHANGES)
            });
            if !fits || chain.is_some_and(|chain| touched(&chain, &family)) {
                batch.deferred.push(offer.root);
                continue;
            }
            rows = taken.unwrap_or(rows);
            batch.families.push(Retiring {
                family,
                bundle: offer.bundle,
                bytes: offer.bytes,
            });
        }
        Ok(batch)
    }

    /// What retiring `families` changes on `view` at its next sequence: every row of each
    /// family deleted, a continuation naming its bundle where each member's claim row was, the
    /// meta, and the batch's one outcome (a `Retirement` invocation of its first root, its
    /// intent the batch's digest). Guarded as admission guards an outcome: one a sequence,
    /// within the core's bound.
    pub(super) fn retirement_batch_changes(
        &self,
        view: &View<'_>,
        families: &[Retiring],
    ) -> Result<(Vec<Change<Key, Row>>, NativeOutcome), NativeError> {
        let first = families.first().ok_or(ContractError::InvalidManifest)?;
        let mut meta = view.meta();
        if u64::try_from(meta.outcomes).ok() != Some(view.prefix().0) {
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
        let sequence = SessionSeq(
            view.prefix()
                .0
                .checked_add(1)
                .ok_or(NativeError::Capacity("native prefix"))?,
        );
        let mut count = 2usize;
        for retiring in families {
            count = count
                .checked_add(retiring.family.rows())
                .and_then(|count| count.checked_add(retiring.family.members.len()))
                .ok_or(NativeError::Capacity("retirement rows"))?;
        }
        let mut changes = Vec::new();
        changes
            .try_reserve_exact(count)
            .map_err(|_| MemoryError::AllocationFailed)?;
        let mut members = 0u32;
        for retiring in families {
            let family = &retiring.family;
            if family.through > view.prefix() {
                return Err(ContractError::InvalidManifest.into());
            }
            for key in &family.keys {
                let Some(row) = view.get(*key) else {
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
                        // A timer's outcome leaving with its family is sealed in its bundle
                        // (F12), its events with it.
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
            for (member, events) in family.members.iter().zip(family.events.iter()) {
                let claim = as_claim(view.get(Key::Claim(*member)))
                    .ok_or(NativeError::Contract(ContractError::InvalidManifest))?;
                changes.push(Change::Put(entry(
                    Key::Retired(*member),
                    Row::Retired(Box::new(RetiredClaim {
                        bundle: retiring.bundle,
                        bytes: retiring.bytes,
                        through: family.through,
                        binding: claim.binding(),
                        status: claim.status(),
                        retired_at: sequence,
                        events: *events,
                    })),
                    0,
                )));
                members = members
                    .checked_add(1)
                    .ok_or(NativeError::Capacity("members"))?;
            }
        }
        meta.outcomes = meta
            .outcomes
            .checked_add(1)
            .ok_or(NativeError::Capacity("outcome rows"))?;
        let outcome = NativeOutcome {
            ledger: self.state.ledger,
            invocation: NativeInvocation::Retirement(first.family.root),
            sequence,
            logical_time: meta.logical_time,
            operation: NativeOperation::Retire,
            intent: batch_digest(self.state.ledger, families)?,
            created: 0,
            changed: members,
            definitions: 0,
            evaluations: 0,
            artifacts: 0,
            results: 0,
            receipts: 0,
            responses: 0,
            result_testaments: 0,
            events: 0,
        };
        changes.push(Change::Put(entry(Key::Meta, Row::Meta(Box::new(meta)), 0)));
        changes.push(Change::Put(entry(
            Key::Outcome(outcome.invocation),
            Row::Outcome(Box::new(OutcomeRow::stored(&outcome, self.state.ledger)?)),
            0,
        )));
        Ok((changes, outcome))
    }

    /// The candidate that retires `families` on `view`, funded from `source` in `lane` as
    /// any candidate is: its changes planned over the state it is prepared on (doc 30 §9),
    /// its write set, its checkpoint accounting and its place in an owner's pending index.
    /// It files no event: a retirement only unfiles its families' events as it publishes.
    pub(super) fn prepare_retirement_batch(
        &self,
        view: &View<'_>,
        families: &[Retiring],
        source: &MemoryBudget,
        lane: BudgetLane,
    ) -> Result<NativePrepared, NativeError> {
        let (changes, outcome) = self.retirement_batch_changes(view, families)?;
        let fragments = view.state.rows.plan_sparse_bounded(
            source,
            view.tail().map(|tail| &tail.fragments),
            outcome.sequence.0,
            changes,
            lane,
            MAX_RETIRE_CHANGES,
            |key| view.get_entry(key).map(|entry| entry.heap_bytes),
        )?;
        let changed = fragments.changes().count();
        let mutation_bytes = super::mutation::bytes(changed)?;
        let writes = super::mutation::WriteSet::capture(
            view.state.profile,
            changed,
            fragments.changes().map(|(_, _, change)| change),
            mutation_bytes,
            source
                .reserve(BudgetKind::Pending, lane, mutation_bytes)?
                .commit(),
        )?;
        let encoded_added = super::encoded_added(
            fragments.changes().map(|(_, _, change)| change),
            view.state.ledger,
        )?;
        let mut removed = super::EncodedRemoved::new(view.state.ledger);
        for (_, _, change) in fragments.changes() {
            if let Some(old) = view.get_entry(change.key()) {
                removed.count(change.key(), &old.value)?;
            }
        }
        writes.check(&fragments)?;
        let index = super::pending_index::entry_charge()
            .checked_mul(changed)
            .ok_or(NativeError::Capacity("pending index charge"))?;
        let index = source.reserve(BudgetKind::Pending, lane, index)?.commit();
        let mut record = Vec::new();
        record
            .try_reserve_exact(families.len())
            .map_err(|_| MemoryError::AllocationFailed)?;
        record.extend(families.iter().map(|retiring| RetiredBundle {
            root: retiring.family.root,
            bundle: retiring.bundle,
            bytes: retiring.bytes,
        }));
        Ok(NativePrepared {
            fragments,
            outcome,
            writes,
            encoded_added,
            encoded_removed: removed.bytes(),
            _index: Some(index),
            events: None,
            retire: Some(record),
        })
    }

    /// Apply a committed retirement record this replica did not prepare (doc 31 M2): its
    /// families derived from the committed rows at the record's place, each one that cannot
    /// leave here, or a batch whose digest, outcome or sequence differ from the record's, a
    /// divergence (`InvalidManifest`), never a retirement skipped; then the same changes the
    /// author's candidate made, published at the record's sequence.
    pub fn apply_retirement_record(
        &mut self,
        record: &record_codec::RetireRecord,
    ) -> Result<NativeOutcome, NativeError> {
        let header = record.header();
        if header.ledger != self.state.ledger
            || header.profile != self.state.profile
            || header.base != self.native_sequence()
        {
            return Err(ContractError::InvalidManifest.into());
        }
        let retired = record.retired();
        if retired.len() > MAX_RETIRE_FAMILIES {
            return Err(NativeError::Capacity("retirement families"));
        }
        let mut families = Vec::new();
        families
            .try_reserve_exact(retired.len())
            .map_err(|_| MemoryError::AllocationFailed)?;
        for one in retired {
            let family = self
                .retirement_family(one.root)
                .map_err(|_| ContractError::InvalidManifest)?;
            if families
                .iter()
                .any(|retiring: &Retiring| retiring.family.members.contains(&one.root))
            {
                return Err(ContractError::InvalidManifest.into());
            }
            families.push(Retiring {
                family,
                bundle: one.bundle,
                bytes: one.bytes,
            });
        }
        let view = View::committed(&self.state);
        let (changes, outcome) = self.retirement_batch_changes(&view, &families)?;
        if outcome != header.outcome {
            return Err(ContractError::InvalidManifest.into());
        }
        for retiring in &families {
            self.unfile_family_events(&retiring.family);
        }
        if let Err(error) = self.state.rows.publish_changes(
            outcome.sequence.0,
            changes,
            BudgetLane::Completion,
            MAX_RETIRE_CHANGES,
        ) {
            self.state.events.drop_all();
            return Err(error);
        }
        Ok(outcome)
    }
}
