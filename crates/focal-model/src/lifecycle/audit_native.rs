//! Native audit construction consumes the complete sealed owner index directly.
//! Evaluations follow registry order; accepted history is grouped in that order,
//! with increasing original accepted revisions inside each group. Publication
//! positions remain the enclosing owner's responsibility, not inferred here.
use super::*;
use crate::lifecycle::aggregation::NativeSealedTargets;
use crate::lifecycle::memory as bytes;
use std::cmp::Ordering;

const ALLOCATION: usize = 4 * std::mem::size_of::<usize>();

#[derive(Debug)]
pub struct NativeAuditPlan<'a, 'definition> {
    targets: NativeSealedTargets<'a>,
    evaluations: &'a [Evaluation<'definition>],
    history: &'a [AcceptedResult],
    capacity: usize,
    heap: usize,
    allocations: usize,
    charge: usize,
    visits: usize,
    remaining: Visits,
}

#[derive(Debug)]
struct Visits(usize);
impl Visits {
    fn take(&mut self, count: usize) -> Result<(), ContractError> {
        self.0 = self.0.checked_sub(count).ok_or(ContractError::Capacity)?;
        Ok(())
    }
}

fn sort_visits(count: usize) -> Result<usize, ContractError> {
    if count < 2 {
        return Ok(0);
    }
    let depth = usize::try_from(
        usize::BITS
            .checked_sub(count.leading_zeros())
            .ok_or(ContractError::Capacity)?,
    )
    .map_err(|_| ContractError::Capacity)?;
    count
        .checked_mul(depth)
        .and_then(|n| n.checked_mul(6))
        .ok_or(ContractError::Capacity)
}

impl AuditCohort {
    /// Allocation-free validation over exact registry-ordered evaluations and
    /// contiguous accepted-history groups. Limits include all source rows;
    /// `max_visits` covers validation, copying and both counted in-place sorts.
    /// The caller holds construction_charge before invoking build.
    pub fn prepare_native<'a, 'definition>(
        targets: NativeSealedTargets<'a>,
        evaluations: &'a [Evaluation<'definition>],
        history: &'a [AcceptedResult],
        limits: Limits,
        max_bytes: usize,
        max_visits: usize,
    ) -> Result<NativeAuditPlan<'a, 'definition>, ContractError> {
        bytes::fits(evaluations.len(), limits.evaluations)?;
        bytes::fits(history.len(), limits.results)?;
        if targets.rows().len() != evaluations.len() {
            return Err(ContractError::InvalidManifest);
        }
        let visits = bytes::add(
            bytes::add(
                evaluations
                    .len()
                    .checked_mul(3)
                    .ok_or(ContractError::Capacity)?,
                history
                    .len()
                    .checked_mul(2)
                    .ok_or(ContractError::Capacity)?,
            )?,
            bytes::add(sort_visits(evaluations.len())?, sort_visits(history.len())?)?,
        )?;
        bytes::fits(visits, max_visits)?;
        let mut remaining = Visits(visits);
        let mut offset = 0usize;
        let mut full_capacity = 0usize;
        let mut complete = true;
        for (registered, evaluation) in targets.rows().iter().zip(evaluations) {
            remaining.take(1)?;
            // The capability already proves the complete immutable policy.
            // Both constructors use the same exact private semantic checks.
            Self::check_registered(targets.claim(), registered, evaluation)?;
            let member = AuditMember::from_evaluation(evaluation);
            complete &= member.complete();
            let bound = usize::try_from(evaluation.attempt_bound().max(1))
                .map_err(|_| ContractError::Capacity)?;
            full_capacity = bytes::add(full_capacity, bound)?;
            let mut last: Option<AcceptedResult> = None;
            let mut count = 0usize;
            while let Some(result) = history.get(offset).copied() {
                if result_key(result) != member.key {
                    break;
                }
                remaining.take(1)?;
                Self::check_result(targets.claim().binding(), &member, result)?;
                if let Some(previous) = last
                    && (previous.is_terminal()
                        || result.binding().revision <= previous.binding().revision)
                {
                    return Err(ContractError::InvalidManifest);
                }
                match result.attempt() {
                    Some(attempt) if usize::try_from(attempt).ok() == Some(count) => {}
                    None if count == 0
                        && matches!(result.phase(), Phase::Delivery | Phase::MissingTarget) => {}
                    _ => return Err(ContractError::InvalidManifest),
                }
                count = bytes::add(count, 1)?;
                bytes::fits(count, bound)?;
                offset = bytes::add(offset, 1)?;
                last = Some(result);
            }
            if last != member.last_result
                || last.is_some_and(|result| {
                    result.is_terminal() && result.resulting_state() != member.state
                })
            {
                return Err(ContractError::InvalidManifest);
            }
        }
        if offset != history.len() {
            return Err(ContractError::InvalidManifest);
        }
        // A fully completed immutable bundle needs no future attempt slots.
        // An open audit retains all admitted capacity for allocation-free record.
        let capacity = if complete {
            history.len()
        } else {
            full_capacity
        };
        bytes::fits(capacity, limits.results)?;
        let heap = bytes::add(
            bytes::array::<AuditMember>(evaluations.len())?,
            bytes::array::<AcceptedResult>(capacity)?,
        )?;
        let allocations = bytes::add(
            bytes::allocation::<AuditMember>(evaluations.len()),
            bytes::allocation::<AcceptedResult>(capacity),
        )?;
        let charge = bytes::add(
            bytes::total::<AuditCohort>(heap)?,
            allocations
                .checked_mul(ALLOCATION)
                .ok_or(ContractError::Capacity)?,
        )?;
        bytes::fits(charge, max_bytes)?;
        Ok(NativeAuditPlan {
            targets,
            evaluations,
            history,
            capacity,
            heap,
            allocations,
            charge,
            visits,
            remaining,
        })
    }
}

impl NativeAuditPlan<'_, '_> {
    /// Includes cohort inline bytes, both complete buffer capacities, and one
    /// conservative allocator header for each nonempty buffer. Source arrays
    /// and the borrowed plan itself remain separately owned by the caller.
    pub fn construction_charge(&self) -> usize {
        self.charge
    }
    pub fn heap_bytes(&self) -> usize {
        self.heap
    }
    pub fn heap_allocations(&self) -> usize {
        self.allocations
    }
    pub fn visits(&self) -> usize {
        self.visits
    }
    pub fn build(mut self) -> Result<AuditCohort, ContractError> {
        let mut members = bytes::reserve::<AuditMember>(self.evaluations.len())?;
        bytes::fits(members.capacity(), self.evaluations.len())?;
        let mut results = bytes::reserve::<AcceptedResult>(self.capacity)?;
        bytes::fits(results.capacity(), self.capacity)?;
        for evaluation in self.evaluations {
            self.remaining.take(1)?;
            members.push(AuditMember::from_evaluation(evaluation));
        }
        for result in self.history {
            self.remaining.take(1)?;
            results.push(*result);
        }
        sort(&mut members, &mut self.remaining, |a, b| {
            a.order.cmp(&b.order)
        })?;
        sort(&mut results, &mut self.remaining, |a, b| {
            result_order(*a).cmp(&result_order(*b))
        })?;
        let mut previous = None;
        for member in &members {
            self.remaining.take(1)?;
            if previous.is_some_and(|order| order >= member.order) {
                return Err(ContractError::InvalidManifest);
            }
            previous = Some(member.order);
        }
        let cohort = AuditCohort {
            claim: self.targets.claim().binding(),
            issuer: self.targets.claim().issuer(),
            sequence: self.targets.sealed_at(),
            members,
            results,
            result_capacity: self.capacity,
        };
        let actual = bytes::add(
            cohort.retained_bytes()?,
            cohort
                .heap_allocations()?
                .checked_mul(ALLOCATION)
                .ok_or(ContractError::Capacity)?,
        )?;
        bytes::fits(actual, self.charge)?;
        Ok(cohort)
    }
}

fn exchange<T: Copy>(
    values: &mut [T],
    left: usize,
    right: usize,
    visits: &mut Visits,
) -> Result<(), ContractError> {
    visits.take(1)?;
    let a = *values.get(left).ok_or(ContractError::Capacity)?;
    let b = *values.get(right).ok_or(ContractError::Capacity)?;
    *values.get_mut(left).ok_or(ContractError::Capacity)? = b;
    *values.get_mut(right).ok_or(ContractError::Capacity)? = a;
    Ok(())
}

fn compare<T>(
    values: &[T],
    left: usize,
    right: usize,
    visits: &mut Visits,
    order: &impl Fn(&T, &T) -> Ordering,
) -> Result<Ordering, ContractError> {
    visits.take(1)?;
    Ok(order(
        values.get(left).ok_or(ContractError::Capacity)?,
        values.get(right).ok_or(ContractError::Capacity)?,
    ))
}

fn sift<T: Copy>(
    values: &mut [T],
    mut root: usize,
    end: usize,
    visits: &mut Visits,
    order: &impl Fn(&T, &T) -> Ordering,
) -> Result<(), ContractError> {
    loop {
        let left = root
            .checked_mul(2)
            .and_then(|n| n.checked_add(1))
            .ok_or(ContractError::Capacity)?;
        if left >= end {
            return Ok(());
        }
        let right = bytes::add(left, 1)?;
        let child = if right < end && compare(values, left, right, visits, order)?.is_lt() {
            right
        } else {
            left
        };
        if !compare(values, root, child, visits, order)?.is_lt() {
            return Ok(());
        }
        exchange(values, root, child, visits)?;
        root = child;
    }
}

fn sort<T: Copy>(
    values: &mut [T],
    visits: &mut Visits,
    order: impl Fn(&T, &T) -> Ordering,
) -> Result<(), ContractError> {
    let mut root = values.len().checked_div(2).ok_or(ContractError::Capacity)?;
    while root != 0 {
        root = root.checked_sub(1).ok_or(ContractError::Capacity)?;
        sift(values, root, values.len(), visits, &order)?;
    }
    let mut end = values.len();
    while end > 1 {
        end = end.checked_sub(1).ok_or(ContractError::Capacity)?;
        exchange(values, 0, end, visits)?;
        sift(values, 0, end, visits, &order)?;
    }
    Ok(())
}
