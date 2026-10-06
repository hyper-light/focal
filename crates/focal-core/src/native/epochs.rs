//! A principal's request generations (the audit's F12). Outcomes are kept
//! resident for what can still ask them through the live path: a request's
//! outcome for as long as its generation is at or above the principal's
//! floor. Below the floor the generation is closed: a request in it is
//! refused `RequestHistoryExpired`, never executed again, and its outcome is
//! in a seal (`seal.rs`) or awaits one. The window is the principal's row
//! under the control affinity: the floor, the generations open for
//! admission — the one a client fills and the one it drains, opened in
//! order by the first request in each — the outcomes each holds, and where
//! the sealed generations' outcomes went.
use super::prepare::{ALLOCATION, Scratch, add, within};
use super::*;
use focal_model::RequestEpoch;

/// Generations a principal may hold open at once: the one it fills and the
/// one it drains while the last operations of it are acknowledged (21 §3).
/// A third is refused until the floor advances.
pub const OPEN_EPOCHS: usize = 2;

/// What an open generation holds: its resident outcomes and the logical
/// time of the last.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OpenEpoch {
    pub outcomes: u32,
    pub last: u64,
}

/// Consecutive sealed generations and the seal (`Key::Seal`) holding their
/// outcomes; a fold rewrites the seal to the row that covers it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealedRange {
    pub first: RequestEpoch,
    pub last: RequestEpoch,
    pub seal: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EpochWindow {
    /// Generations below the floor are closed.
    pub floor: RequestEpoch,
    /// Generations below this have their outcomes in a seal; the ones in
    /// `sealed..floor` are closed and await one.
    pub sealed: RequestEpoch,
    /// How many generations are open: `floor..floor + open`.
    pub open: u8,
    pub counts: [OpenEpoch; OPEN_EPOCHS],
    ranges: Vec<SealedRange>,
}

impl EpochWindow {
    /// A principal never seen: its first generation is one.
    pub fn first() -> Self {
        Self {
            floor: RequestEpoch(1),
            sealed: RequestEpoch(1),
            open: 0,
            counts: [OpenEpoch::default(); OPEN_EPOCHS],
            ranges: Vec::new(),
        }
    }
    /// A decoded window; refused unless its invariants hold.
    pub fn new(
        floor: RequestEpoch,
        sealed: RequestEpoch,
        open: u8,
        counts: [OpenEpoch; OPEN_EPOCHS],
        ranges: Vec<SealedRange>,
    ) -> Result<Self, NativeError> {
        let window = Self {
            floor,
            sealed,
            open,
            counts,
            ranges,
        };
        if !window.valid() {
            return Err(ContractError::InvalidManifest.into());
        }
        Ok(window)
    }
    pub fn valid(&self) -> bool {
        let open = usize::from(self.open);
        let below_sealed = self
            .ranges
            .last()
            .is_none_or(|range| range.last.0 < self.sealed.0);
        let ordered = self.ranges.windows(2).all(|pair| match pair {
            [previous, next] => previous.last.0 < next.first.0 && previous.seal < next.seal,
            _ => false,
        });
        self.floor.0 >= 1
            && self.sealed.0 >= 1
            && self.sealed.0 <= self.floor.0
            && open <= OPEN_EPOCHS
            && self.floor.0.checked_add(self.open.into()).is_some()
            && self
                .ranges
                .iter()
                .all(|range| range.first.0 >= 1 && range.first.0 <= range.last.0 && range.seal >= 1)
            && below_sealed
            && ordered
            && self
                .counts
                .iter()
                .skip(open)
                .all(|count| *count == OpenEpoch::default())
    }
    /// The generation the next open would take.
    pub fn next(&self) -> RequestEpoch {
        RequestEpoch(self.floor.0.saturating_add(self.open.into()))
    }
    /// Resident outcomes across the open generations: what the principal
    /// holds of the live window.
    pub fn open_outcomes(&self) -> usize {
        self.counts
            .iter()
            .take(usize::from(self.open))
            .fold(0usize, |sum, count| {
                sum.saturating_add(usize::try_from(count.outcomes).unwrap_or(usize::MAX))
            })
    }
    /// The logical time of the principal's last admitted request; zero when
    /// nothing is open.
    pub fn last_activity(&self) -> u64 {
        self.counts
            .iter()
            .take(usize::from(self.open))
            .map(|count| count.last)
            .max()
            .unwrap_or(0)
    }
    /// The closed generations whose outcomes await a seal: `sealed..floor`.
    pub fn awaiting_seal(&self) -> Option<(RequestEpoch, RequestEpoch)> {
        (self.sealed.0 < self.floor.0).then_some((self.sealed, self.floor))
    }
    pub fn ranges(&self) -> &[SealedRange] {
        &self.ranges
    }
    /// The seal holding a sealed generation's outcomes.
    pub fn seal_of(&self, epoch: RequestEpoch) -> Option<u64> {
        let at = self.ranges.partition_point(|range| range.last.0 < epoch.0);
        self.ranges
            .get(at)
            .filter(|range| range.first.0 <= epoch.0)
            .map(|range| range.seal)
    }
    /// Admit a request in `epoch` at `logical_time`: the generation must be
    /// at or above the floor (`RequestHistoryExpired` below it), open, or the
    /// next one while fewer than [`OPEN_EPOCHS`] are open
    /// (`EpochNotAdmitted` otherwise); the open generations together hold
    /// fewer than `share` outcomes (`Capacity`: the principal's share of the
    /// live window).
    pub fn admit(
        &mut self,
        epoch: RequestEpoch,
        logical_time: u64,
        share: usize,
    ) -> Result<(), NativeError> {
        if epoch.0 == 0 {
            return Err(ContractError::InvalidTarget.into());
        }
        if epoch.0 < self.floor.0 {
            return Err(ContractError::RequestHistoryExpired.into());
        }
        let next = self.next();
        if epoch.0 > next.0 || (epoch == next && usize::from(self.open) >= OPEN_EPOCHS) {
            return Err(ContractError::EpochNotAdmitted.into());
        }
        if self.open_outcomes() >= share {
            return Err(NativeError::Capacity("principal outcomes"));
        }
        if epoch == next {
            self.open = self
                .open
                .checked_add(1)
                .ok_or(NativeError::Capacity("open epochs"))?;
        }
        let at = usize::try_from(
            epoch
                .0
                .checked_sub(self.floor.0)
                .ok_or(NativeError::Capacity("epoch"))?,
        )
        .map_err(|_| NativeError::Capacity("epoch"))?;
        let count = self
            .counts
            .get_mut(at)
            .ok_or(NativeError::Capacity("open epochs"))?;
        count.outcomes = count
            .outcomes
            .checked_add(1)
            .ok_or(NativeError::Capacity("epoch outcomes"))?;
        count.last = count.last.max(logical_time);
        Ok(())
    }
    /// Advance the floor to `minimum`: above the floor, at most the next
    /// generation (closing every open one). The generations below leave the
    /// window; their outcomes await a seal.
    pub fn advance(&mut self, minimum: RequestEpoch) -> Result<(), NativeError> {
        if minimum.0 <= self.floor.0 || minimum.0 > self.next().0 {
            return Err(ContractError::InvalidTransition.into());
        }
        let closed = usize::try_from(minimum.0.saturating_sub(self.floor.0))
            .map_err(|_| NativeError::Capacity("epoch"))?;
        self.counts.rotate_left(closed.min(OPEN_EPOCHS));
        for count in self
            .counts
            .iter_mut()
            .skip(OPEN_EPOCHS.saturating_sub(closed))
        {
            *count = OpenEpoch::default();
        }
        self.open = self
            .open
            .saturating_sub(u8::try_from(closed).unwrap_or(u8::MAX));
        self.floor = minimum;
        Ok(())
    }
    /// The generations awaiting a seal went into `seal`: one more range,
    /// within `limit` ranges (`Capacity` at the bound — a fold makes room).
    pub fn record_seal(&mut self, seal: u64, limit: usize) -> Result<(), NativeError> {
        let Some((first, floor)) = self.awaiting_seal() else {
            return Err(ContractError::InvalidTransition.into());
        };
        if self.ranges.len() >= limit {
            return Err(NativeError::Capacity("sealed ranges"));
        }
        if self.ranges.len() == self.ranges.capacity() {
            self.ranges
                .try_reserve_exact(1)
                .map_err(|_| NativeError::Memory(MemoryError::AllocationFailed))?;
        }
        self.ranges.push(SealedRange {
            first,
            last: RequestEpoch(floor.0.saturating_sub(1)),
            seal,
        });
        self.sealed = floor;
        Ok(())
    }
    /// Seals `first..=last` were folded into the row keyed `last`: the ranges
    /// they held are covered by it, and adjacent ones merge.
    pub fn fold(&mut self, first: u64, last: u64) {
        for range in &mut self.ranges {
            if (first..=last).contains(&range.seal) {
                range.seal = last;
            }
        }
        self.ranges.dedup_by(|next, previous| {
            if previous.seal == next.seal && previous.last.0.checked_add(1) == Some(next.first.0) {
                previous.last = next.last;
                true
            } else {
                false
            }
        });
    }
    /// The heap the row holds beyond its inline size.
    pub fn heap_charge(&self) -> Result<usize, NativeError> {
        heap(self.ranges.capacity())
    }
    /// The heap a window with `ranges` ranges holds.
    pub fn heap_of(ranges: usize) -> Result<usize, NativeError> {
        heap(ranges)
    }
    /// An exact copy on a heap of its own, funded by the row's own charge
    /// (a retained neighbour a page copy carries; `prepare::copy`).
    pub fn copy(&self) -> Result<Self, MemoryError> {
        let mut ranges = Vec::new();
        ranges
            .try_reserve_exact(self.ranges.len())
            .map_err(|_| MemoryError::AllocationFailed)?;
        ranges.extend_from_slice(&self.ranges);
        if heap(ranges.capacity()).map_err(|_| MemoryError::AllocationFailed)?
            > self
                .heap_charge()
                .map_err(|_| MemoryError::AllocationFailed)?
        {
            return Err(MemoryError::AllocationFailed);
        }
        Ok(Self {
            floor: self.floor,
            sealed: self.sealed,
            open: self.open,
            counts: self.counts,
            ranges,
        })
    }
    /// An exact copy on a heap of its own, charged to `scratch`.
    pub(super) fn try_copy(&self, scratch: &mut Scratch) -> Result<Self, NativeError> {
        let charge = heap(self.ranges.len())?;
        scratch.charge(charge)?;
        let mut ranges = Vec::new();
        ranges
            .try_reserve_exact(self.ranges.len())
            .map_err(|_| NativeError::Memory(MemoryError::AllocationFailed))?;
        ranges.extend_from_slice(&self.ranges);
        within(heap(ranges.capacity())?, charge)?;
        Ok(Self {
            floor: self.floor,
            sealed: self.sealed,
            open: self.open,
            counts: self.counts,
            ranges,
        })
    }
}
fn heap(capacity: usize) -> Result<usize, NativeError> {
    add(
        capacity
            .checked_mul(size_of::<SealedRange>())
            .ok_or(NativeError::Capacity("sealed ranges"))?,
        if capacity == 0 { 0 } else { ALLOCATION },
    )
}

/// The scratch a window's copy takes at most: its ranges at the bound.
pub(super) fn window_bytes(limits: NativeLimits) -> Result<usize, NativeError> {
    heap(limits.seals)
}

/// A principal's share of the live window: the window divided among the
/// principals with one, and at least one — as an identity's share of the
/// listener's ingress (10 §5): no principal takes the window from the
/// others, and one alone may take it all.
pub(super) fn share(limits: NativeLimits, principals: usize) -> usize {
    limits
        .outcomes
        .checked_div(principals.max(1))
        .unwrap_or(0)
        .max(1)
}

impl View<'_> {
    pub(super) fn epochs(&self, principal: ParticipantId) -> Option<&EpochWindow> {
        match self.get(Key::Epochs(principal)) {
            Some(Row::Epochs(window)) => Some(window),
            _ => None,
        }
    }
}

/// The fence every fresh request passes after its exact-retry lookup: what
/// [`admit`] would refuse, without a copy.
pub(super) fn check(
    view: &View<'_>,
    meta: Meta,
    request: RequestKey,
    limits: NativeLimits,
) -> Result<(), NativeError> {
    let (mut window, principals) = match view.epochs(request.principal) {
        Some(window) => (
            EpochWindow {
                floor: window.floor,
                sealed: window.sealed,
                open: window.open,
                counts: window.counts,
                ranges: Vec::new(),
            },
            meta.principals,
        ),
        None => {
            let principals = add(meta.principals, 1)?;
            if principals > limits.principals {
                return Err(NativeError::Capacity("principals"));
            }
            (EpochWindow::first(), principals)
        }
    };
    window.admit(request.epoch, 0, share(limits, principals))
}

/// Admit a request's generation against its principal's window: the window
/// as it will be once the request is published, copied onto scratch, and
/// the meta's count of principals if this is the first. A window never
/// seen starts at generation one.
pub(super) fn admit(
    view: &View<'_>,
    meta: &mut Meta,
    request: RequestKey,
    logical_time: u64,
    limits: NativeLimits,
    scratch: &mut Scratch,
) -> Result<EpochWindow, NativeError> {
    let mut window = match view.epochs(request.principal) {
        Some(window) => window.try_copy(scratch)?,
        None => {
            let principals = add(meta.principals, 1)?;
            if principals > limits.principals {
                return Err(NativeError::Capacity("principals"));
            }
            meta.principals = principals;
            EpochWindow::first()
        }
    };
    window.admit(request.epoch, logical_time, share(limits, meta.principals))?;
    Ok(window)
}

/// The window a staged request's plan carries, for tests that assemble a
/// plan by hand: what the build admits for the outcome's request.
#[cfg(test)]
pub(super) fn staged(
    view: &View<'_>,
    meta: &mut Meta,
    outcome: NativeOutcome,
    limits: NativeLimits,
) -> Option<(ParticipantId, EpochWindow)> {
    let NativeInvocation::Request(request) = outcome.invocation else {
        return None;
    };
    let mut scratch = Scratch {
        used: 0,
        max: limits.preparation_bytes,
    };
    let window = admit(
        view,
        meta,
        request,
        outcome.logical_time,
        limits,
        &mut scratch,
    )
    .ok()?;
    Some((request.principal, window))
}

/// `AdvanceEpochFloor { minimum }`: the principal's own window moves its
/// floor; the request that carries it is admitted in a generation at or
/// above the new floor like any other, so the window in the plan is the
/// admitted one advanced. No claim row changes.
pub(super) fn prepare_advance(
    context: NativeContext,
    request: RequestKey,
    minimum: RequestEpoch,
    admitted: &mut EpochWindow,
) -> Result<transactions::Plan, NativeError> {
    context.principal.require_actor(request.principal)?;
    if request.epoch.0 < minimum.0 {
        // The request itself would be below the floor it asks for.
        return Err(ContractError::InvalidTransition.into());
    }
    admitted.advance(minimum)?;
    Ok(transactions::Plan {
        rows: Vec::new(),
        registry: transactions::RegistryOverrides::new(),
        created: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generations_open_in_order_two_at_a_time_and_close_below_the_floor() {
        let mut window = EpochWindow::first();
        assert!(matches!(
            window.admit(RequestEpoch(2), 1, usize::MAX),
            Err(NativeError::Contract(ContractError::EpochNotAdmitted))
        ));
        window.admit(RequestEpoch(1), 1, usize::MAX).unwrap();
        window.admit(RequestEpoch(1), 2, usize::MAX).unwrap();
        window.admit(RequestEpoch(2), 3, usize::MAX).unwrap();
        assert_eq!(window.open, 2);
        assert_eq!(window.open_outcomes(), 3);
        assert_eq!(window.last_activity(), 3);
        assert!(matches!(
            window.admit(RequestEpoch(3), 4, usize::MAX),
            Err(NativeError::Contract(ContractError::EpochNotAdmitted))
        ));
        window.advance(RequestEpoch(2)).unwrap();
        assert_eq!(window.open, 1);
        assert_eq!(window.counts[0].outcomes, 1);
        assert_eq!(
            window.awaiting_seal(),
            Some((RequestEpoch(1), RequestEpoch(2)))
        );
        assert!(matches!(
            window.admit(RequestEpoch(1), 5, usize::MAX),
            Err(NativeError::Contract(ContractError::RequestHistoryExpired))
        ));
        window.admit(RequestEpoch(3), 5, usize::MAX).unwrap();
        assert!(matches!(
            window.advance(RequestEpoch(2)),
            Err(NativeError::Contract(ContractError::InvalidTransition))
        ));
        window.advance(RequestEpoch(4)).unwrap();
        assert_eq!(window.open, 0);
        assert_eq!(window.next(), RequestEpoch(4));
        assert!(window.valid());
    }

    #[test]
    fn a_principal_holds_its_share_and_no_more() {
        let mut window = EpochWindow::first();
        window.admit(RequestEpoch(1), 1, 2).unwrap();
        window.admit(RequestEpoch(1), 1, 2).unwrap();
        assert!(matches!(
            window.admit(RequestEpoch(1), 1, 2),
            Err(NativeError::Capacity("principal outcomes"))
        ));
        let limits = NativeLimits {
            outcomes: 100,
            ..NativeLimits::default()
        };
        assert_eq!(share(limits, 0), 100);
        assert_eq!(share(limits, 3), 33);
        assert_eq!(share(limits, 1000), 1);
    }

    #[test]
    fn sealed_ranges_name_their_seal_and_fold_onto_the_covering_row() {
        let mut window = EpochWindow::first();
        window.admit(RequestEpoch(1), 1, usize::MAX).unwrap();
        window.advance(RequestEpoch(2)).unwrap();
        window.record_seal(1, 8).unwrap();
        window.admit(RequestEpoch(2), 2, usize::MAX).unwrap();
        window.admit(RequestEpoch(3), 3, usize::MAX).unwrap();
        window.advance(RequestEpoch(4)).unwrap();
        window.record_seal(2, 8).unwrap();
        assert!(window.record_seal(3, 8).is_err());
        assert_eq!(window.seal_of(RequestEpoch(1)), Some(1));
        assert_eq!(window.seal_of(RequestEpoch(3)), Some(2));
        assert_eq!(window.seal_of(RequestEpoch(4)), None);
        assert_eq!(window.ranges().len(), 2);
        window.fold(1, 2);
        assert_eq!(window.ranges().len(), 1);
        assert_eq!(window.seal_of(RequestEpoch(1)), Some(2));
        assert_eq!(window.seal_of(RequestEpoch(3)), Some(2));
        assert!(window.valid());
        let mut scratch = Scratch { used: 0, max: 1024 };
        let copy = window.try_copy(&mut scratch).unwrap();
        assert_eq!(copy, window);
        assert_eq!(scratch.used, copy.heap_charge().unwrap());
        assert!(window.try_copy(&mut Scratch { used: 0, max: 1 }).is_err());
    }
}
