use crate::*;
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use focal_model::{LedgerId, RouteEpoch, SessionSeq};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PinRequest {
    pub prefix: SessionSeq,
    pub query: QueryId,
    pub span: KeySpan,
    pub now: u64,
    pub ttl: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeCursor {
    pub ledger: LedgerId,
    pub incarnation: ControllerIncarnation,
    pub pin: u64,
    pub query: QueryId,
    pub epoch: RouteEpoch,
    pub prefix: SessionSeq,
    pub expires_at: u64,
    pub after: Option<StorageKey>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadFragment {
    pub span: KeySpan,
    pub after_exclusive: Option<StorageKey>,
    pub availability: ReadAvailability,
}
pub struct RangeReadPlan {
    pub cursor: RangeCursor,
    pub fragments: Vec<ReadFragment>,
    _allocation: Allocation,
}
struct Pin {
    map: RangeMap,
    span: KeySpan,
    cursor: RangeCursor,
    proofs: Vec<ReadAvailability>,
    _allocation: Allocation,
}
pub(crate) struct PinRegistry {
    incarnation: ControllerIncarnation,
    pins: BTreeMap<u64, Pin>,
    next: u64,
    clock: u64,
    limits: RangeLimits,
    budget: MemoryBudget,
}
impl PinRegistry {
    pub fn new(
        incarnation: ControllerIncarnation,
        limits: RangeLimits,
        budget: MemoryBudget,
    ) -> Self {
        Self {
            incarnation,
            pins: BTreeMap::new(),
            next: 0,
            clock: 0,
            limits,
            budget,
        }
    }
    pub fn advance(&mut self, now: u64) -> Result<(), RangeError> {
        if now < self.clock {
            return Err(RangeError::ClockRegression);
        }
        self.clock = now;
        self.pins.retain(|_, pin| pin.cursor.expires_at > now);
        Ok(())
    }
    pub fn pin(
        &mut self,
        map: &RangeMap,
        request: PinRequest,
        proofs: &[ReadAvailability],
        verifier: &impl RangeVerifier,
    ) -> Result<RangeCursor, RangeError> {
        let PinRequest {
            prefix,
            query,
            span,
            now,
            ttl,
        } = request;
        self.advance(now)?;
        span.validate()?;
        if ttl == 0 || ttl > self.limits.max_pin_ttl || self.pins.len() == self.limits.max_pins {
            return Err(RangeError::Capacity);
        }
        let expires_at = now.checked_add(ttl).ok_or(RangeError::Overflow)?;
        let id = self.next.checked_add(1).ok_or(RangeError::Overflow)?;
        let cursor = RangeCursor {
            ledger: map.ledger(),
            incarnation: self.incarnation,
            pin: id,
            query,
            epoch: map.epoch(),
            prefix,
            expires_at,
            after: None,
        };
        validate_availability(map, span, &cursor, proofs, self.limits, verifier)?;
        let bytes = add(
            row::<(u64, Pin)>(),
            add(
                map.charge()?,
                mul(proofs.len(), size_of::<ReadAvailability>())?,
            )?,
        )?;
        let allocation = self
            .budget
            .reserve(BudgetKind::ReadPins, BudgetLane::Ordinary, bytes)?
            .commit();
        self.pins.insert(
            id,
            Pin {
                map: map.clone(),
                span,
                cursor: cursor.clone(),
                proofs: proofs.to_vec(),
                _allocation: allocation,
            },
        );
        self.next = id;
        Ok(cursor)
    }
    pub fn plan(
        &mut self,
        cursor: &RangeCursor,
        now: u64,
        target: Option<(&RangeMap, &[ReadAvailability])>,
        verifier: &impl RangeVerifier,
    ) -> Result<RangeReadPlan, RangeError> {
        self.advance(now)?;
        let pin = self.checked(cursor)?;
        let (map, proofs) = target.unwrap_or((&pin.map, &pin.proofs));
        if map.ledger() != cursor.ledger {
            return Err(RangeError::WrongLedger);
        }
        validate_availability(map, pin.span, cursor, proofs, self.limits, verifier)?;
        let allocation = self
            .budget
            .reserve(
                BudgetKind::Query,
                BudgetLane::Ordinary,
                add(
                    size_of::<RangeReadPlan>(),
                    mul(map.ranges().len(), size_of::<ReadFragment>())?,
                )?,
            )?
            .commit();
        let mut fragments = Vec::new();
        fragments
            .try_reserve_exact(map.ranges().len())
            .map_err(|_| RangeError::Capacity)?;
        for range in map.ranges() {
            let Some(span) = range.span.intersection(&pin.span) else {
                continue;
            };
            if cursor
                .after
                .is_some_and(|after| span.end.is_some_and(|end| after >= end))
            {
                continue;
            }
            let proof = proofs
                .iter()
                .find(|proof| proof.range == range.id)
                .ok_or(RangeError::ReadTooOld)?;
            fragments.push(ReadFragment {
                span,
                after_exclusive: cursor
                    .after
                    .filter(|after| span.start.is_none_or(|start| *after >= start)),
                availability: proof.clone(),
            });
        }
        Ok(RangeReadPlan {
            cursor: cursor.clone(),
            fragments,
            _allocation: allocation,
        })
    }
    pub fn release(&mut self, cursor: &RangeCursor) -> Result<(), RangeError> {
        self.checked(cursor)?;
        self.pins.remove(&cursor.pin);
        Ok(())
    }
    pub fn pinned(&self, epoch: RouteEpoch) -> bool {
        self.pins.values().any(|pin| pin.cursor.epoch == epoch)
    }
    fn checked(&self, cursor: &RangeCursor) -> Result<&Pin, RangeError> {
        if cursor.incarnation != self.incarnation {
            return Err(RangeError::ReadTooOld);
        }
        let pin = self.pins.get(&cursor.pin).ok_or(RangeError::Expired)?;
        let expected = &pin.cursor;
        if cursor.ledger != expected.ledger
            || cursor.query != expected.query
            || cursor.epoch != expected.epoch
            || cursor.prefix != expected.prefix
            || cursor.expires_at != expected.expires_at
        {
            return Err(RangeError::Conflict);
        }
        if cursor.after.is_some_and(|after| !pin.span.contains(&after)) {
            return Err(RangeError::Conflict);
        }
        Ok(pin)
    }
}
fn validate_availability(
    map: &RangeMap,
    span: KeySpan,
    cursor: &RangeCursor,
    proofs: &[ReadAvailability],
    limits: RangeLimits,
    verifier: &impl RangeVerifier,
) -> Result<(), RangeError> {
    if proofs.len() > limits.max_ranges {
        return Err(RangeError::Capacity);
    }
    let required = map
        .ranges()
        .iter()
        .filter(|range| range.span.intersection(&span).is_some());
    if proofs.len() != required.clone().count() {
        return Err(RangeError::ReadTooOld);
    }
    for range in required {
        let mut matching = proofs.iter().filter(|proof| proof.range == range.id);
        let proof = matching.next().ok_or(RangeError::ReadTooOld)?;
        if matching.next().is_some()
            || proof.ledger != cursor.ledger
            || proof.epoch != map.epoch()
            || proof.range_generation != range.generation
            || !range.meta.accepts_reader(proof.replica)
            || proof.prefix != cursor.prefix
            || proof.lease == 0
            || proof.expires_at < cursor.expires_at
            || !nonzero(proof.root)
            || !nonzero(proof.attestation)
        {
            return Err(RangeError::ReadTooOld);
        }
        verifier.read(proof)?;
    }
    Ok(())
}
