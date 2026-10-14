//! Seals (the audit's F12): the outcomes nothing can ask again through the
//! live path leave the core for a bundle under custody, and the core keeps a
//! bounded index over the bundles. An outcome is closed when a request in
//! its generation is refused below its principal's floor (`epochs.rs`), when
//! it is a retirement's or a seal's own (their fences are the `Retired` and
//! `Seal` rows), or — for a timer — when its claim retired, in which case
//! it left with the family's bundle at retirement (`retirement.rs`). A seal
//! is a session decision applied alike on every replica: derived from the
//! committed state at a prefix, so the record names the bundle and the
//! bounds of the derivation, never the rows.
use super::prepare::{Scratch, add, within};
use super::*;
use focal_memory::Change;
use focal_model::RequestEpoch;

/// The most bytes one seal bundle takes: the archive read's bound
/// (`archive_reads::MAX_ARCHIVE_BYTES`), so a seal is read as a bundle is.
pub const SEAL_BUNDLE_BYTES: usize = 6 << 20;
/// What a fold bundle spends per member at most: the member's row and, per
/// principal, the range that points into it.
const FOLD_MEMBER_BYTES: usize = 80 + 4096 * size_of::<SealedRange>();
/// Resident seal rows before the oldest half fold into one: twice the
/// members one fold bundle holds within its bound, so a fold always fits.
pub const DEFAULT_SEAL_ROWS: usize = 2 * (SEAL_BUNDLE_BYTES / FOLD_MEMBER_BYTES);
/// An encoded request outcome row: the row's kind and key (a tag, the
/// invocation's namespace, principal, generation and id), its length and
/// its body (ledger, invocation, sequence, time, operation, intent and ten
/// counts) — what a seal bundle spends per outcome.
pub const OUTCOME_ROW_BYTES: usize =
    1 + (1 + 1 + 16 + 8 + 16) + 4 + (32 + 42 + 8 + 8 + 1 + 32 + 40);
/// The rows one seal bundle holds at most: the bundle's bound over an
/// outcome row; a bundle with creation results among its rows is halved
/// until it fits.
pub const DEFAULT_SEAL_ROWS_PER_BUNDLE: usize = SEAL_BUNDLE_BYTES / OUTCOME_ROW_BYTES;

/// A seal's row (`Key::Seal(ordinal)`): its bundle, what it holds and what
/// it covers. A fold's row is keyed by the last ordinal it covers and names
/// the first; a seal's `first` is its own ordinal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealRow {
    pub bundle: ContentHash,
    pub bytes: u64,
    /// The native prefix the seal was derived at: every outcome it holds is
    /// at or below it.
    pub through: SessionSeq,
    /// Outcome rows the bundle holds (a fold: across its members).
    pub count: u64,
    pub sealed_at: SessionSeq,
    pub first: u64,
}
impl SealRow {
    pub fn valid(&self, ordinal: u64) -> bool {
        // A seal's row holds a bundle and the rows in it; a fold's, its
        // directory (whose members may all be closing seals); a closing
        // seal's (F12 level 5), nothing.
        let holds = self.bundle.0 != [0; 32]
            && self.bytes != 0
            && (self.count != 0 || self.is_fold(ordinal));
        (holds || self.is_closing(ordinal))
            && self.through.0 != 0
            && self.sealed_at > self.through
            && ordinal != 0
            && self.first != 0
            && self.first <= ordinal
    }
    pub fn is_fold(&self, ordinal: u64) -> bool {
        self.first < ordinal
    }
    /// A closing seal's row (F12 level 5): its ordinal forced floors and
    /// sealed nothing, so it names no bundle; no window points into it.
    pub fn is_closing(&self, ordinal: u64) -> bool {
        self.first == ordinal && self.bundle.0 == [0; 32] && self.bytes == 0 && self.count == 0
    }
}

/// One principal's generations a seal holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealedPrincipal {
    pub principal: ParticipantId,
    pub first: RequestEpoch,
    pub last: RequestEpoch,
}

/// How much one seal takes at most: principals whose closed generations it
/// holds, and rows. Named by the record, so every replica derives the same
/// seal from the same prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealBound {
    pub principals: usize,
    pub rows: usize,
}

/// A fold named by a seal record: the seal rows `first..=last` become one
/// row keyed `last`, whose bundle is a directory of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fold {
    pub first: u64,
    pub last: u64,
    pub bundle: ContentHash,
    pub bytes: u64,
}

/// What a seal record says, checked against the committed state at apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealRecord<'a> {
    /// The floors the seal forces under pressure: exactly
    /// [`Core::pressure_floors`] for their count, or the record is inert.
    pub floors: &'a [(ParticipantId, RequestEpoch)],
    pub bound: SealBound,
    pub bundle: ContentHash,
    pub bytes: u64,
    pub count: u64,
    pub fold: Option<Fold>,
}

/// What an anchored seal record says (F12, level 5), checked against the
/// committed state where it applies: the plan it names was derived at
/// `through` and holds only what could not change since, so commits during
/// its custody leave it applicable; the floors are the pressure's where it
/// applies. A closing seal names floors and seals nothing (`count` zero, no
/// bundle): what it closes is sealed whole by a later seal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnchoredRecord<'a> {
    /// The floors the pressure forces where the record applies: exactly
    /// [`Core::close_floors`] for their count.
    pub floors: &'a [(ParticipantId, RequestEpoch)],
    /// The ordinal the bundle was derived under: the next one where the
    /// record applies.
    pub ordinal: u64,
    /// The native prefix the plan was derived at.
    pub through: SessionSeq,
    /// The generations sealed, by principal in order.
    pub principals: &'a [SealedPrincipal],
    /// The rows the plan took: what bounds the decisions' outcomes it takes
    /// again.
    pub count: u64,
    pub bundle: ContentHash,
    pub bytes: u64,
    pub fold: Option<Fold>,
}

/// How a seal's floors meet its plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Sealing {
    /// The plan was derived under the floors: they close first, and the
    /// seal takes what they closed.
    Derived,
    /// The plan names what was closed where it was derived: the floors
    /// close after its ranges are recorded.
    Anchored,
}

/// The rows a seal takes, derived from the committed state: deterministic
/// over the prefix, the floors and the bound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealPlan {
    pub through: SessionSeq,
    pub ordinal: u64,
    /// By principal: the closed generations sealed, `sealed..floor` of each
    /// window once the floors apply.
    pub principals: Vec<SealedPrincipal>,
    pub(super) keys: Vec<Key>,
    pub creation_results: usize,
}
impl SealPlan {
    pub fn rows(&self) -> usize {
        self.keys.len()
    }
    /// Outcome rows among the rows.
    pub fn outcomes(&self) -> usize {
        self.keys.len().saturating_sub(self.creation_results)
    }
}

/// What a fold takes: the member rows and the ranges that point into them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoldPlan {
    pub first: u64,
    pub last: u64,
    pub members: Vec<(u64, SealRow)>,
    pub ranges: Vec<(ParticipantId, SealedRange)>,
    pub through: SessionSeq,
    pub count: u64,
}

/// Why nothing is sealed now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SealRefusal {
    /// No closed outcome awaits a seal.
    Nothing,
    /// The floors named are not the pressure floors of this state.
    Floors,
    /// A window or a seal row the state holds is not one the core made.
    Corrupt,
    Capacity,
}
impl From<NativeError> for SealRefusal {
    fn from(error: NativeError) -> Self {
        match error {
            NativeError::Capacity(_) | NativeError::Memory(_) => Self::Capacity,
            _ => Self::Corrupt,
        }
    }
}

const ZERO: ParticipantId = ParticipantId([0; 16]);

impl Core<NativeState> {
    /// Resident outcomes: the lifetime count less what left into seals and
    /// retirement bundles.
    pub fn resident_outcomes(&self) -> usize {
        let meta = self.meta_row();
        meta.outcomes.saturating_sub(meta.sealed)
    }
    fn meta_row(&self) -> Meta {
        match self.state.rows.get(&Key::Meta) {
            Some(Row::Meta(meta)) => **meta,
            _ => Meta::default(),
        }
    }
    /// A principal's request generation window, if it has one.
    pub fn native_epochs(&self, principal: ParticipantId) -> Option<&EpochWindow> {
        match self.state.rows.get(&Key::Epochs(principal)) {
            Some(Row::Epochs(window)) => Some(window),
            _ => None,
        }
    }
    /// The seal row covering `ordinal`: the first row keyed at or after it
    /// whose `first` is at or before it.
    pub fn native_seal(&self, ordinal: u64) -> Option<(u64, SealRow)> {
        self.state
            .rows
            .entries_from(&Key::Seal(ordinal), false)
            .next()
            .and_then(|entry| match (entry.key, &entry.value) {
                (Key::Seal(key), Row::Seal(row)) if row.first <= ordinal && ordinal <= key => {
                    Some((key, **row))
                }
                _ => None,
            })
    }
    /// The ordinal the next seal takes; `None` past the counter.
    pub fn native_seal_ordinal(&self) -> Option<u64> {
        u64::try_from(self.meta_row().seals)
            .ok()
            .and_then(|seals| seals.checked_add(1))
    }
    /// The core's native limits: what the seal agent sizes its bundles and
    /// folds by.
    pub fn native_limits(&self) -> NativeLimits {
        self.limits
    }
    /// The resident seal rows in ordinal order, at most one more than the
    /// bound (a state past it is refused by recovery).
    pub fn native_seal_rows(&self) -> Result<Vec<(u64, SealRow)>, NativeError> {
        let mut rows = Vec::new();
        rows.try_reserve_exact(self.limits.seals.saturating_add(1))
            .map_err(|_| NativeError::Memory(MemoryError::AllocationFailed))?;
        for entry in self.state.rows.entries_from(&Key::Seal(0), false) {
            let (Key::Seal(ordinal), Row::Seal(row)) = (entry.key, &entry.value) else {
                break;
            };
            if rows.len() == rows.capacity() {
                return Err(NativeError::Capacity("seal rows"));
            }
            rows.push((ordinal, **row));
        }
        Ok(rows)
    }
    /// The windows, by principal: every `Epochs` row under the control
    /// affinity, each visited once.
    fn windows(&self) -> impl Iterator<Item = (ParticipantId, &EpochWindow)> {
        self.state
            .rows
            .entries_from(&Key::Epochs(ZERO), false)
            .map_while(|entry| match (entry.key, &entry.value) {
                (Key::Epochs(principal), Row::Epochs(window)) => Some((principal, &**window)),
                _ => None,
            })
    }
    /// The floors the pressure of the live window forces, at most `max` of
    /// them: when the resident outcomes and the candidates that may still
    /// be admitted would pass the bound, the open generations least
    /// recently used close first — by the logical time of their last
    /// request, then by principal — until what they hold covers the excess.
    /// Deterministic over the committed state, so a record naming them is
    /// checked by deriving them again. Empty under no pressure.
    pub fn pressure_floors(
        &self,
        max: usize,
    ) -> Result<Vec<(ParticipantId, RequestEpoch)>, NativeError> {
        let mut floors = Vec::new();
        let meta = self.meta_row();
        let resident = meta.outcomes.saturating_sub(meta.sealed);
        let excess = add(resident, self.limits.pending)?.saturating_sub(self.limits.outcomes);
        if excess == 0 || max == 0 {
            return Ok(floors);
        }
        let mut candidates: Vec<(u64, ParticipantId, RequestEpoch, usize)> = Vec::new();
        candidates
            .try_reserve_exact(meta.principals.min(self.limits.principals))
            .map_err(|_| NativeError::Memory(MemoryError::AllocationFailed))?;
        for (principal, window) in self.windows() {
            if window.open == 0 {
                continue;
            }
            if candidates.len() == candidates.capacity() {
                return Err(NativeError::Capacity("principals"));
            }
            candidates.push((
                window.last_activity(),
                principal,
                window.next(),
                window.open_outcomes(),
            ));
        }
        candidates.sort_unstable();
        let mut covered = 0usize;
        for (_, principal, next, held) in candidates {
            if covered >= excess || floors.len() >= max {
                break;
            }
            if floors.len() == floors.capacity() {
                floors
                    .try_reserve_exact(
                        max.min(self.limits.principals)
                            .saturating_sub(floors.len())
                            .max(1),
                    )
                    .map_err(|_| NativeError::Memory(MemoryError::AllocationFailed))?;
            }
            floors.push((principal, next));
            covered = add(covered, held)?;
        }
        Ok(floors)
    }
    /// Whether the live window is under pressure by the anchored rule (F12,
    /// level 5): its resident outcomes, and `admitting` more, within a
    /// quarter of the bound. What the owner checks as it admits, before it
    /// asks [`Core::close_floors`] what to close.
    pub fn close_pressure(&self, admitting: usize) -> bool {
        let bound = self.limits.outcomes;
        self.resident_outcomes()
            .saturating_add(admitting)
            .saturating_add(bound / 4)
            > bound
    }
    /// The floors the pressure of the live window forces by the anchored
    /// rule (F12, level 5), at most `max` of them. Within a quarter of the
    /// bound, the open generations least recently used close — by the
    /// logical time of their last request, then by principal — until the
    /// outcomes awaiting a seal would bring the window back to half once
    /// sealed. What awaits a seal already counts first, so a window whose
    /// seal is on its way forces nothing more, and a closed generation is
    /// sealed whole by a later seal: closing never waits for custody. The
    /// quarter is the slack sealing has while the window still admits;
    /// half is where a pressure ends, so one pressure closes enough for its
    /// seals instead of a generation per admission. Deterministic over the
    /// committed state, so a record naming them is checked by deriving them
    /// again; the count of what awaits stops once it covers the need.
    pub fn close_floors(
        &self,
        max: usize,
    ) -> Result<Vec<(ParticipantId, RequestEpoch)>, NativeError> {
        let mut floors = Vec::new();
        if max == 0 || !self.close_pressure(0) {
            return Ok(floors);
        }
        let meta = self.meta_row();
        let need = self
            .resident_outcomes()
            .saturating_sub(self.limits.outcomes / 2);
        let mut covered = self.awaiting_outcomes(need)?;
        if covered >= need {
            return Ok(floors);
        }
        let mut candidates: Vec<(u64, ParticipantId, RequestEpoch, usize)> = Vec::new();
        candidates
            .try_reserve_exact(meta.principals.min(self.limits.principals))
            .map_err(|_| NativeError::Memory(MemoryError::AllocationFailed))?;
        for (principal, window) in self.windows() {
            if window.open == 0 {
                continue;
            }
            if candidates.len() == candidates.capacity() {
                return Err(NativeError::Capacity("principals"));
            }
            candidates.push((
                window.last_activity(),
                principal,
                window.next(),
                window.open_outcomes(),
            ));
        }
        candidates.sort_unstable();
        for (_, principal, next, held) in candidates {
            if covered >= need || floors.len() >= max {
                break;
            }
            if floors.len() == floors.capacity() {
                floors
                    .try_reserve_exact(
                        max.min(self.limits.principals)
                            .saturating_sub(floors.len())
                            .max(1),
                    )
                    .map_err(|_| NativeError::Memory(MemoryError::AllocationFailed))?;
            }
            floors.push((principal, next));
            covered = add(covered, held)?;
        }
        Ok(floors)
    }
    /// How many more resident outcomes the window takes before the anchored
    /// rule forces a floor ([`Core::close_floors`]), so the owner counts
    /// down instead of asking the rule at each admission: to the start of
    /// the pressure, or, under it, what awaits a seal past the need, counted
    /// up to an eighth of the bound past it; zero when the rule forces a
    /// floor now. A seal or a close changes the count; the owner asks again
    /// after each.
    pub fn close_slack(&self) -> Result<usize, NativeError> {
        let bound = self.limits.outcomes;
        let resident = self.resident_outcomes();
        let onset = bound.saturating_sub(bound / 4);
        if resident <= onset {
            return Ok(onset.saturating_sub(resident).saturating_add(1));
        }
        let need = resident.saturating_sub(bound / 2);
        let covered = self.awaiting_outcomes(add(need, bound / 8)?)?;
        Ok(if covered < need {
            0
        } else {
            covered.saturating_sub(need).saturating_add(1)
        })
    }
    /// The resident outcomes a seal may take now, counted up to `limit`:
    /// every closed generation's awaiting one (`sealed..floor` of each
    /// window), then the retirements' and seals' own. Visits at most
    /// `limit` rows and every window once.
    fn awaiting_outcomes(&self, limit: usize) -> Result<usize, NativeError> {
        let mut count = 0usize;
        for (principal, window) in self.windows() {
            if count >= limit {
                return Ok(count);
            }
            let Some((sealed, floor)) = window.awaiting_seal() else {
                continue;
            };
            let first = Key::Outcome(NativeInvocation::Request(RequestKey {
                principal,
                epoch: sealed,
                id: RequestId([0; 16]),
            }));
            for entry in self.state.rows.entries_from(&first, false) {
                let Key::Outcome(NativeInvocation::Request(key)) = entry.key else {
                    break;
                };
                if key.principal != principal || key.epoch >= floor || count >= limit {
                    break;
                }
                count = add(count, 1)?;
            }
        }
        for first in [
            Key::Outcome(NativeInvocation::Retirement(ClaimId([0; 16]))),
            Key::Outcome(NativeInvocation::Seal(0)),
        ] {
            for entry in self.state.rows.entries_from(&first, false) {
                let same = matches!(
                    (first, entry.key),
                    (
                        Key::Outcome(NativeInvocation::Retirement(_)),
                        Key::Outcome(NativeInvocation::Retirement(_))
                    ) | (
                        Key::Outcome(NativeInvocation::Seal(_)),
                        Key::Outcome(NativeInvocation::Seal(_))
                    )
                );
                if !same || count >= limit {
                    break;
                }
                count = add(count, 1)?;
            }
        }
        Ok(count)
    }
    /// The window of `principal` once `floors` apply: copied onto `scratch`
    /// when it changes, borrowed otherwise.
    fn window_after<'w>(
        &'w self,
        principal: ParticipantId,
        window: &'w EpochWindow,
        floors: &[(ParticipantId, RequestEpoch)],
        scratch: &mut Scratch,
    ) -> Result<std::borrow::Cow<'w, EpochWindow>, NativeError> {
        match floors.iter().find(|(p, _)| *p == principal) {
            Some((_, minimum)) => {
                let mut copied = window.try_copy(scratch)?;
                copied.advance(*minimum)?;
                Ok(std::borrow::Cow::Owned(copied))
            }
            None => Ok(std::borrow::Cow::Borrowed(window)),
        }
    }
    /// Derive the seal the committed state yields under `floors` and
    /// `bound`: by principal, the outcome and creation-result rows of the
    /// generations awaiting a seal (whole generations, whole principals,
    /// the first `bound.principals` with any), then every retirement's and
    /// seal's own outcome; `bound.rows` rows at most, in key order.
    pub fn seal_plan(
        &self,
        floors: &[(ParticipantId, RequestEpoch)],
        bound: SealBound,
    ) -> Result<SealPlan, SealRefusal> {
        if self.pressure_floors(floors.len())? != floors {
            return Err(SealRefusal::Floors);
        }
        let meta = self.meta_row();
        let through = self.native_sequence();
        let ordinal = u64::try_from(meta.seals)
            .ok()
            .and_then(|seals| seals.checked_add(1))
            .ok_or(SealRefusal::Capacity)?;
        let mut scratch = Scratch {
            used: 0,
            max: self.limits.preparation_bytes,
        };
        let mut keys: Vec<Key> = Vec::new();
        keys.try_reserve_exact(bound.rows.min(self.resident_outcomes().saturating_mul(2)))
            .map_err(|_| SealRefusal::Capacity)?;
        let mut principals: Vec<SealedPrincipal> = Vec::new();
        let mut creation_results = 0usize;
        for (principal, window) in self.windows() {
            if principals.len() >= bound.principals {
                break;
            }
            let window = self.window_after(principal, window, floors, &mut scratch)?;
            let Some((sealed, floor)) = window.awaiting_seal() else {
                continue;
            };
            let start = keys.len();
            let mut fits = true;
            for family in [false, true] {
                let first = RequestKey {
                    principal,
                    epoch: sealed,
                    id: RequestId([0; 16]),
                };
                let first = if family {
                    Key::CreationResult(NativeInvocation::Request(first))
                } else {
                    Key::Outcome(NativeInvocation::Request(first))
                };
                for entry in self.state.rows.entries_from(&first, false) {
                    let invocation = match (family, entry.key) {
                        (false, Key::Outcome(invocation))
                        | (true, Key::CreationResult(invocation)) => invocation,
                        _ => break,
                    };
                    let NativeInvocation::Request(key) = invocation else {
                        break;
                    };
                    if key.principal != principal || key.epoch >= floor {
                        break;
                    }
                    if keys.len() >= bound.rows || keys.len() == keys.capacity() {
                        fits = false;
                        break;
                    }
                    keys.push(entry.key);
                    if family {
                        creation_results = add(creation_results, 1)?;
                    }
                }
                if !fits {
                    break;
                }
            }
            if !fits {
                // Whole generations or nothing of this principal; the next
                // seal takes it.
                let dropped = keys.len().saturating_sub(start);
                creation_results = creation_results.saturating_sub(
                    keys.iter()
                        .skip(start)
                        .filter(|key| matches!(key, Key::CreationResult(_)))
                        .count(),
                );
                keys.truncate(start);
                if dropped != 0 {
                    break;
                }
                continue;
            }
            if keys.len() == start {
                continue;
            }
            if principals.len() == principals.capacity() {
                principals
                    .try_reserve_exact(
                        bound
                            .principals
                            .min(self.limits.principals)
                            .saturating_sub(principals.len())
                            .max(1),
                    )
                    .map_err(|_| SealRefusal::Capacity)?;
            }
            principals.push(SealedPrincipal {
                principal,
                first: sealed,
                last: RequestEpoch(floor.0.saturating_sub(1)),
            });
        }
        for first in [
            Key::Outcome(NativeInvocation::Retirement(ClaimId([0; 16]))),
            Key::Outcome(NativeInvocation::Seal(0)),
        ] {
            for entry in self.state.rows.entries_from(&first, false) {
                match (first, entry.key) {
                    (
                        Key::Outcome(NativeInvocation::Retirement(_)),
                        Key::Outcome(NativeInvocation::Retirement(_)),
                    )
                    | (
                        Key::Outcome(NativeInvocation::Seal(_)),
                        Key::Outcome(NativeInvocation::Seal(_)),
                    ) => {}
                    _ => break,
                }
                if keys.len() >= bound.rows || keys.len() == keys.capacity() {
                    break;
                }
                keys.push(entry.key);
            }
        }
        if keys.is_empty() {
            return Err(SealRefusal::Nothing);
        }
        keys.sort_unstable();
        Ok(SealPlan {
            through,
            ordinal,
            principals,
            keys,
            creation_results,
        })
    }
    /// The fold the committed state yields for `first..=last`: the seal
    /// rows keyed in that span, which must be exactly the rows covering it
    /// from `first` on, and every range of every window pointing into them.
    pub fn fold_plan(&self, first: u64, last: u64) -> Result<FoldPlan, SealRefusal> {
        if first == 0 || first >= last {
            return Err(SealRefusal::Corrupt);
        }
        let mut members: Vec<(u64, SealRow)> = Vec::new();
        members
            .try_reserve_exact(self.limits.seals)
            .map_err(|_| SealRefusal::Capacity)?;
        let mut expected = first;
        for entry in self.state.rows.entries_from(&Key::Seal(first), false) {
            let (Key::Seal(ordinal), Row::Seal(row)) = (entry.key, &entry.value) else {
                break;
            };
            if ordinal > last {
                break;
            }
            if row.first != expected || members.len() == members.capacity() {
                return Err(SealRefusal::Corrupt);
            }
            members.push((ordinal, **row));
            expected = ordinal.checked_add(1).ok_or(SealRefusal::Corrupt)?;
        }
        if members.len() < 2 || members.last().is_none_or(|(ordinal, _)| *ordinal != last) {
            return Err(SealRefusal::Corrupt);
        }
        let mut ranges: Vec<(ParticipantId, SealedRange)> = Vec::new();
        for (principal, window) in self.windows() {
            for range in window.ranges() {
                if (first..=last).contains(&range.seal) {
                    if ranges.len() == ranges.capacity() {
                        ranges
                            .try_reserve_exact(self.limits.seals.max(1))
                            .map_err(|_| SealRefusal::Capacity)?;
                    }
                    ranges.push((principal, *range));
                }
            }
        }
        let count = members
            .iter()
            .try_fold(0u64, |sum, (_, row)| sum.checked_add(row.count))
            .ok_or(SealRefusal::Capacity)?;
        let through = members
            .last()
            .map(|(_, row)| row.through)
            .ok_or(SealRefusal::Corrupt)?;
        Ok(FoldPlan {
            first,
            last,
            members,
            ranges,
            through,
            count,
        })
    }
    fn plan_entries<'a>(&'a self, plan: &'a SealPlan) -> impl Iterator<Item = (Key, &'a Row)> + 'a {
        plan.keys
            .iter()
            .filter_map(|key| self.state.rows.get(key).map(|row| (*key, row)))
    }
    /// Measure the seal's bundle.
    pub fn seal_quote(
        &self,
        plan: &SealPlan,
        limits: record_codec::EncodingLimits,
    ) -> Result<retirement::ArchiveQuote, record_codec::CodecError> {
        let mut sink = record_codec::counting_sink(limits.bytes, limits.visits);
        let hash = record_codec::seal_frame(
            &mut sink,
            record_codec::checkpoint::SealFrame::Seal {
                ledger: self.state.ledger,
                profile: self.state.profile,
                through: plan.through,
                ordinal: plan.ordinal,
                principals: &plan.principals,
                count: plan.keys.len(),
            },
            self.plan_entries(plan),
        )?;
        Ok(retirement::ArchiveQuote {
            bytes: sink.len(),
            visits: sink.visits_used(),
            hash,
        })
    }
    /// Write the seal's bundle into `output` (exactly the quoted bytes).
    pub fn seal_into(
        &self,
        plan: &SealPlan,
        output: &mut [u8],
        visits: usize,
    ) -> Result<ContentHash, record_codec::CodecError> {
        let mut sink = record_codec::slice_sink(output, visits);
        let hash = record_codec::seal_frame(
            &mut sink,
            record_codec::checkpoint::SealFrame::Seal {
                ledger: self.state.ledger,
                profile: self.state.profile,
                through: plan.through,
                ordinal: plan.ordinal,
                principals: &plan.principals,
                count: plan.keys.len(),
            },
            self.plan_entries(plan),
        )?;
        sink.finish()?;
        Ok(hash)
    }
    /// Measure a fold's directory bundle.
    pub fn fold_quote(
        &self,
        plan: &FoldPlan,
        limits: record_codec::EncodingLimits,
    ) -> Result<retirement::ArchiveQuote, record_codec::CodecError> {
        let mut sink = record_codec::counting_sink(limits.bytes, limits.visits);
        let hash = record_codec::seal_frame(
            &mut sink,
            record_codec::checkpoint::SealFrame::Fold {
                ledger: self.state.ledger,
                profile: self.state.profile,
                first: plan.first,
                last: plan.last,
                members: &plan.members,
                ranges: &plan.ranges,
            },
            std::iter::empty(),
        )?;
        Ok(retirement::ArchiveQuote {
            bytes: sink.len(),
            visits: sink.visits_used(),
            hash,
        })
    }
    /// Write a fold's directory bundle into `output`.
    pub fn fold_into(
        &self,
        plan: &FoldPlan,
        output: &mut [u8],
        visits: usize,
    ) -> Result<ContentHash, record_codec::CodecError> {
        let mut sink = record_codec::slice_sink(output, visits);
        let hash = record_codec::seal_frame(
            &mut sink,
            record_codec::checkpoint::SealFrame::Fold {
                ledger: self.state.ledger,
                profile: self.state.profile,
                first: plan.first,
                last: plan.last,
                members: &plan.members,
                ranges: &plan.ranges,
            },
            std::iter::empty(),
        )?;
        sink.finish()?;
        Ok(hash)
    }
    /// Whether the outcome a seal publishes fits: one more resident outcome
    /// before the sealed ones leave.
    pub fn check_seal_outcome(&self) -> Result<(), SealRefusal> {
        let resident = self.resident_outcomes();
        if resident
            .checked_add(1)
            .is_none_or(|next| next > self.limits.outcomes.saturating_add(self.limits.pending))
        {
            return Err(SealRefusal::Capacity);
        }
        Ok(())
    }
    /// Apply a committed seal (F12): derive the same plan from the committed
    /// state, require the record's count, and publish at the next native
    /// prefix: the plan's rows leave, each sealed principal's window
    /// records the seal, the seal's row is written (and a fold replaces the
    /// rows it covers, every window's ranges following), the meta counts
    /// the sealed rows and the seal, and the seal's own outcome is written.
    /// Returns how many rows left.
    pub fn apply_seal(&mut self, record: SealRecord<'_>) -> Result<usize, NativeError> {
        let corrupt = || NativeError::Contract(ContractError::InvalidManifest);
        let meta = self.meta_row();
        if u64::try_from(meta.outcomes).ok() != Some(self.native_sequence().0) {
            return Err(corrupt());
        }
        let plan =
            self.seal_plan(record.floors, record.bound)
                .map_err(|refusal| match refusal {
                    SealRefusal::Capacity => NativeError::Capacity("seal"),
                    _ => corrupt(),
                })?;
        if u64::try_from(plan.keys.len()).ok() != Some(record.count)
            || record.bundle.0 == [0; 32]
            || record.bytes == 0
        {
            return Err(corrupt());
        }
        self.publish_seal(
            &plan,
            Sealing::Derived,
            record.floors,
            record.bundle,
            record.bytes,
            record.fold,
        )
    }
    /// Apply a committed anchored seal (F12, level 5): the floors are the
    /// pressure's here ([`Core::close_floors`] for their count, or
    /// `Floors`), the plan is the one the record names derived again here
    /// ([`Core::anchored_plan`]) under the ordinal it was derived for, and
    /// the publication is a derived seal's but for the floors, which close
    /// after the seal's ranges are recorded: what they close awaits a later
    /// seal. A closing seal names floors and nothing to seal: its row holds
    /// no bundle, and its outcome is published as any seal's. Returns how
    /// many rows left.
    pub fn apply_anchored(&mut self, record: AnchoredRecord<'_>) -> Result<usize, NativeError> {
        let corrupt = || NativeError::Contract(ContractError::InvalidManifest);
        let meta = self.meta_row();
        if u64::try_from(meta.outcomes).ok() != Some(self.native_sequence().0) {
            return Err(corrupt());
        }
        if self.close_floors(record.floors.len())? != record.floors {
            return Err(corrupt());
        }
        let ordinal = u64::try_from(meta.seals)
            .ok()
            .and_then(|seals| seals.checked_add(1))
            .ok_or(NativeError::Capacity("seals"))?;
        if record.ordinal != ordinal || record.through > self.native_sequence() {
            return Err(corrupt());
        }
        let closing = record.count == 0;
        let plan = if closing {
            if record.floors.is_empty()
                || !record.principals.is_empty()
                || record.bundle.0 != [0; 32]
                || record.bytes != 0
                || record.fold.is_some()
                || record.through != self.native_sequence()
            {
                return Err(corrupt());
            }
            SealPlan {
                through: record.through,
                ordinal,
                principals: Vec::new(),
                keys: Vec::new(),
                creation_results: 0,
            }
        } else {
            if record.bundle.0 == [0; 32] || record.bytes == 0 {
                return Err(corrupt());
            }
            let rows = usize::try_from(record.count).map_err(|_| corrupt())?;
            let plan = self
                .anchored_plan(record.through, record.principals, rows)
                .map_err(|refusal| match refusal {
                    SealRefusal::Capacity => NativeError::Capacity("seal"),
                    _ => corrupt(),
                })?;
            if plan.keys.len() != rows {
                return Err(corrupt());
            }
            plan
        };
        self.publish_seal(
            &plan,
            Sealing::Anchored,
            record.floors,
            record.bundle,
            record.bytes,
            record.fold,
        )
    }
    /// The plan an anchored seal names (F12, level 5), derived again where
    /// it applies: by principal, every outcome and creation-result row of
    /// the generations it names — each range starting at its window's
    /// sealed mark and closed — then the retirements' and seals' own
    /// outcomes published at or below `through`, `rows` rows at most in key
    /// order. A closed generation's rows never change and a decision's
    /// outcome is never rewritten, so the plan derived at `through` with no
    /// floors ([`Core::seal_plan`]) is this plan wherever the record
    /// applies, whatever was admitted since; a range another seal took, or
    /// one that is not closed, is refused (`Corrupt`).
    pub fn anchored_plan(
        &self,
        through: SessionSeq,
        principals: &[SealedPrincipal],
        rows: usize,
    ) -> Result<SealPlan, SealRefusal> {
        let meta = self.meta_row();
        let ordinal = u64::try_from(meta.seals)
            .ok()
            .and_then(|seals| seals.checked_add(1))
            .ok_or(SealRefusal::Capacity)?;
        if through > self.native_sequence() {
            return Err(SealRefusal::Corrupt);
        }
        let mut keys: Vec<Key> = Vec::new();
        keys.try_reserve_exact(rows.min(self.resident_outcomes().saturating_mul(2)))
            .map_err(|_| SealRefusal::Capacity)?;
        let mut named: Vec<SealedPrincipal> = Vec::new();
        named
            .try_reserve_exact(principals.len())
            .map_err(|_| SealRefusal::Capacity)?;
        let mut creation_results = 0usize;
        for sealed in principals {
            if named
                .last()
                .is_some_and(|previous| previous.principal >= sealed.principal)
            {
                return Err(SealRefusal::Corrupt);
            }
            let window = self
                .native_epochs(sealed.principal)
                .ok_or(SealRefusal::Corrupt)?;
            let (first, floor) = window.awaiting_seal().ok_or(SealRefusal::Corrupt)?;
            if sealed.first != first || sealed.last < sealed.first || sealed.last >= floor {
                return Err(SealRefusal::Corrupt);
            }
            for family in [false, true] {
                let start = RequestKey {
                    principal: sealed.principal,
                    epoch: sealed.first,
                    id: RequestId([0; 16]),
                };
                let start = if family {
                    Key::CreationResult(NativeInvocation::Request(start))
                } else {
                    Key::Outcome(NativeInvocation::Request(start))
                };
                for entry in self.state.rows.entries_from(&start, false) {
                    let invocation = match (family, entry.key) {
                        (false, Key::Outcome(invocation))
                        | (true, Key::CreationResult(invocation)) => invocation,
                        _ => break,
                    };
                    let NativeInvocation::Request(key) = invocation else {
                        break;
                    };
                    if key.principal != sealed.principal || key.epoch > sealed.last {
                        break;
                    }
                    // More rows than the record took: not the plan it names.
                    if keys.len() >= rows || keys.len() == keys.capacity() {
                        return Err(SealRefusal::Corrupt);
                    }
                    keys.push(entry.key);
                    if family {
                        creation_results = add(creation_results, 1)?;
                    }
                }
            }
            named.push(*sealed);
        }
        for first in [
            Key::Outcome(NativeInvocation::Retirement(ClaimId([0; 16]))),
            Key::Outcome(NativeInvocation::Seal(0)),
        ] {
            for entry in self.state.rows.entries_from(&first, false) {
                match (first, entry.key) {
                    (
                        Key::Outcome(NativeInvocation::Retirement(_)),
                        Key::Outcome(NativeInvocation::Retirement(_)),
                    )
                    | (
                        Key::Outcome(NativeInvocation::Seal(_)),
                        Key::Outcome(NativeInvocation::Seal(_)),
                    ) => {}
                    _ => break,
                }
                if keys.len() >= rows || keys.len() == keys.capacity() {
                    break;
                }
                let Row::Outcome(outcome) = &entry.value else {
                    return Err(SealRefusal::Corrupt);
                };
                if outcome.sequence <= through {
                    keys.push(entry.key);
                }
            }
        }
        if keys.is_empty() {
            return Err(SealRefusal::Nothing);
        }
        keys.sort_unstable();
        Ok(SealPlan {
            through,
            ordinal,
            principals: named,
            keys,
            creation_results,
        })
    }
    /// What a checked seal changes, published at the next native prefix:
    /// the plan's rows leave; every window that changes is written once —
    /// the fold first (what makes room in a window at the range bound for
    /// the seal that carries it), the seal's range, and the floors, which a
    /// derived seal applies before its range (it takes what they close) and
    /// an anchored one after it (a later seal takes what they close); the
    /// seal's row (a closing seal's holds no bundle) and a fold's; the
    /// meta; the seal's own outcome. Returns how many rows left.
    fn publish_seal(
        &mut self,
        plan: &SealPlan,
        sealing: Sealing,
        floors: &[(ParticipantId, RequestEpoch)],
        bundle: ContentHash,
        bytes: u64,
        fold: Option<Fold>,
    ) -> Result<usize, NativeError> {
        let corrupt = || NativeError::Contract(ContractError::InvalidManifest);
        let mut meta = self.meta_row();
        let folding = fold
            .map(|fold| {
                if fold.bundle.0 == [0; 32] || fold.bytes == 0 || fold.last >= plan.ordinal {
                    return Err(corrupt());
                }
                self.fold_plan(fold.first, fold.last)
                    .map_err(|refusal| match refusal {
                        SealRefusal::Capacity => NativeError::Capacity("fold"),
                        _ => corrupt(),
                    })
            })
            .transpose()?;
        let sequence = SessionSeq(
            self.native_sequence()
                .0
                .checked_add(1)
                .ok_or(NativeError::Capacity("sequence"))?,
        );
        let mut scratch = Scratch {
            used: 0,
            max: self.limits.preparation_bytes,
        };
        // Every window that changes: the floors forced, the seal recorded,
        // the fold followed; each at most once.
        let mut windows: Vec<(ParticipantId, EpochWindow)> = Vec::new();
        windows
            .try_reserve_exact(meta.principals.min(self.limits.principals))
            .map_err(|_| NativeError::Memory(MemoryError::AllocationFailed))?;
        for (principal, window) in self.windows() {
            let forced = floors.iter().find(|(p, _)| *p == principal);
            let sealed = plan
                .principals
                .iter()
                .find(|entry| entry.principal == principal);
            let folded = folding.as_ref().is_some_and(|fold| {
                window
                    .ranges()
                    .iter()
                    .any(|range| (fold.first..=fold.last).contains(&range.seal))
            });
            if forced.is_none() && sealed.is_none() && !folded {
                continue;
            }
            let mut copied = window.try_copy(&mut scratch)?;
            if sealing == Sealing::Derived
                && let Some((_, minimum)) = forced
            {
                copied.advance(*minimum)?;
            }
            if let Some(fold) = &folding {
                copied.fold(fold.first, fold.last);
            }
            match (sealing, sealed) {
                (Sealing::Derived, Some(_)) => {
                    copied.record_seal(plan.ordinal, self.limits.seals)?;
                }
                (Sealing::Anchored, Some(entry)) => {
                    copied.record_seal_through(plan.ordinal, entry.last, self.limits.seals)?;
                }
                (_, None) => {}
            }
            if sealing == Sealing::Anchored
                && let Some((_, minimum)) = forced
            {
                copied.advance(*minimum)?;
            }
            if windows.len() == windows.capacity() {
                return Err(NativeError::Capacity("principals"));
            }
            windows.push((principal, copied));
        }
        let mut changes: Vec<Change<Key, Row>> = Vec::new();
        let count = add(
            add(plan.keys.len(), windows.len())?,
            add(3, folding.as_ref().map_or(0, |fold| fold.members.len()))?,
        )?;
        changes
            .try_reserve_exact(count)
            .map_err(|_| NativeError::Memory(MemoryError::AllocationFailed))?;
        let mut sealed_events = 0usize;
        for key in &plan.keys {
            if let Some(Row::Outcome(outcome)) = self.state.rows.get(key) {
                sealed_events = add(
                    sealed_events,
                    usize::try_from(outcome.events).map_err(|_| NativeError::Capacity("events"))?,
                )?;
            }
            changes.push(Change::Delete(*key));
        }
        for (principal, window) in windows {
            let heap = window.heap_charge()?;
            changes.push(Change::Put(entry(
                Key::Epochs(principal),
                Row::Epochs(Box::new(window)),
                heap,
            )));
        }
        if let Some(plan) = &folding {
            for (ordinal, _) in &plan.members {
                if *ordinal != plan.last {
                    changes.push(Change::Delete(Key::Seal(*ordinal)));
                }
            }
            let Some(folded) = fold else {
                return Err(corrupt());
            };
            changes.push(Change::Put(entry(
                Key::Seal(plan.last),
                Row::Seal(Box::new(SealRow {
                    bundle: folded.bundle,
                    bytes: folded.bytes,
                    through: plan.through,
                    count: plan.count,
                    sealed_at: sequence,
                    first: plan.first,
                })),
                0,
            )));
        }
        let held =
            u64::try_from(plan.keys.len()).map_err(|_| NativeError::Capacity("sealed rows"))?;
        changes.push(Change::Put(entry(
            Key::Seal(plan.ordinal),
            Row::Seal(Box::new(SealRow {
                bundle,
                bytes,
                through: plan.through,
                count: held,
                sealed_at: sequence,
                first: plan.ordinal,
            })),
            0,
        )));
        meta.sealed = add(meta.sealed, plan.outcomes())?;
        meta.sealed_events = add(meta.sealed_events, sealed_events)?;
        meta.creation_results = meta
            .creation_results
            .checked_sub(plan.creation_results)
            .ok_or_else(corrupt)?;
        meta.seals = add(meta.seals, 1)?;
        meta.outcomes = add(meta.outcomes, 1)?;
        within(
            meta.outcomes.saturating_sub(meta.sealed),
            self.limits.outcomes.saturating_add(self.limits.pending),
        )?;
        let mut hasher = blake3::Hasher::new_derive_key("focal.native.seal.outcome.v1");
        hasher.update(&self.state.ledger.tenant.0);
        hasher.update(&self.state.ledger.session.0);
        hasher.update(&plan.ordinal.to_le_bytes());
        hasher.update(&bundle.0);
        hasher.update(&plan.through.0.to_le_bytes());
        let outcome = NativeOutcome {
            ledger: self.state.ledger,
            invocation: NativeInvocation::Seal(plan.ordinal),
            sequence,
            logical_time: meta.logical_time,
            operation: NativeOperation::Seal,
            intent: ContentHash(*hasher.finalize().as_bytes()),
            created: 0,
            changed: u32::try_from(plan.keys.len())
                .map_err(|_| NativeError::Capacity("sealed rows"))?,
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
        let left = plan.keys.len();
        // The seal publishes straight to the root: its rows' running total moves with it.
        let encoded = self.encoded_after_changes(&changes)?;
        // One publication of what the seal's bound allows: a bundle's rows,
        // every window at most once, the fold's rows, the seal's row, the
        // meta and the outcome.
        let bound = DEFAULT_SEAL_ROWS_PER_BUNDLE
            .saturating_add(self.limits.principals)
            .saturating_add(self.limits.seals)
            .saturating_add(3);
        self.state.rows.publish_changes(
            sequence.0,
            changes,
            focal_memory::BudgetLane::Committed,
            bound,
        )?;
        self.state.encoded_rows = encoded;
        Ok(left)
    }
}
