//! Shape-only bounds for any legal future effective prefix. The owner keeps
//! growth inside this shape and separately funds the quote before accepting work.
use super::*;

/// Inclusive future maxima, not a selection of currently visible evidence.
/// Work includes unclosed and failed rows; evaluations include every registered
/// family, whether Ready, retrying, suppressed, fenced, or terminal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectionShape {
    pub responses: usize,
    pub works: usize,
    pub evaluations: usize,
}

/// Additional projection peak and the two independent counted-visit ceilings.
/// This is neither a memory reservation nor evaluation authority. Its bounds
/// exclude source-owned rows, sorting comparisons, uncounted policy helpers and
/// adapter work such as lookup descent, overlay searches and membership cursors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectionQuote {
    shape: ProjectionShape,
    bytes: usize,
    inspection: usize,
    reduction: usize,
}
impl ProjectionQuote {
    pub fn shape(&self) -> ProjectionShape {
        self.shape
    }
    pub fn construction_charge(&self) -> usize {
        self.bytes
    }
    pub fn inspection_visits(&self) -> usize {
        self.inspection
    }
    pub fn reduction_visits(&self) -> usize {
        self.reduction
    }
}

fn add(a: usize, b: usize) -> Result<usize, ContractError> {
    a.checked_add(b).ok_or(ContractError::Capacity)
}
fn mul(a: usize, b: usize) -> Result<usize, ContractError> {
    a.checked_mul(b).ok_or(ContractError::Capacity)
}
fn sum<const N: usize>(terms: [usize; N]) -> Result<usize, ContractError> {
    terms.into_iter().try_fold(0, add)
}
fn triangular(n: usize) -> Result<usize, ContractError> {
    let Some(previous) = n.checked_sub(1) else {
        return Ok(0);
    };
    // Divide the even factor first, so a representable triangular count does
    // not fail merely because its undivided product would overflow.
    if n.is_multiple_of(2) {
        mul(n.checked_div(2).ok_or(ContractError::Capacity)?, previous)
    } else {
        mul(n, previous.checked_div(2).ok_or(ContractError::Capacity)?)
    }
}

/// Quote all response/declared-slot cells, including missing and zero-check
/// slots. The immutable claim supplies slots, declarations and total checks;
/// caller-promised occupancy supplies r/w/e. Every later state must stay within
/// those maxima. Retryable and terminal results use the same all-members bound.
///
/// `limits.visits` applies independently to inspection and reduction, matching
/// the existing runtime counters. No source rows are traversed or allocated.
pub fn quote_projection(
    claim: &ClaimState,
    shape: ProjectionShape,
    limits: ProjectionLimits,
) -> Result<ProjectionQuote, ContractError> {
    let r = shape.responses;
    let w = shape.works;
    let e = shape.evaluations;
    let policy = claim.acceptance();
    let s = policy.slot_count();
    let d = policy.declarations().len();
    let t = mul(r, s)?;
    if r < claim.response_count()
        || r > usize::try_from(claim.max_responses()).map_err(|_| ContractError::Capacity)?
        || r > limits.responses
        || w > limits.works
        || t > limits.slots
        || e > limits.evaluations
        || d > limits.declarations
    {
        return Err(ContractError::Capacity);
    }
    // Match source inspection's cheap refusal before traversing any policies.
    // With r=0 the response-cell bound is zero even for many immutable slots,
    // so it cannot substitute for this independent d+s+k visit preflight.
    let mut policy_visits = Visits::new(limits.visits);
    policy_visits.take(d)?;
    policy_visits.take(s)?;
    let mut k = 0;
    for slot in &policy.slots {
        policy_visits.take(slot.checks.len())?;
        k = add(k, slot.checks.len())?;
    }
    let h = add(r, e)?;
    let a = add(e, t)?;
    // Reuse the exact eleven-buffer layout, including simultaneous temporary
    // buffers, allocator metadata, and the inline projection header.
    let bytes = reduce::construction_charge(
        claim,
        Counts {
            responses: r,
            slots: t,
            results: e,
            events: h,
        },
    )?;
    let e1 = add(e, 1)?;
    let de = mul(d, e1)?;
    // Source terms follow inspect's phases: policy/definitions, complete
    // response chain, member identity/publication uniqueness, Admission cohort,
    // complete work membership, and original response-entry gates.
    let inspection = sum([
        sum([d, s, k])?,
        mul(d, add(d, 1)?)?,
        mul(r, sum([1, mul(s, add(w, 1)?)?, de])?)?,
        triangular(r)?,
        mul(e, sum([mul(2, d)?, 7, w, r])?)?,
        mul(3, triangular(e)?)?,
        e,
        de,
        mul(w, sum([2, r, s, de])?)?,
        triangular(w)?,
        d,
        mul(r, add(1, mul(e, add(d, 4)?)?)?)?,
    ])?;
    // At most h chronological groups and h dirty-response reductions in total:
    // every dirty response needs an entry/result event. Across all groups, each
    // cell contributes at most one terminal/missing cause, charged as t*a.
    let per_group = sum([
        mul(2, r)?,
        mul(2, t)?,
        mul(e, add(a, 1)?)?,
        s,
        mul(add(mul(3, d)?, k)?, e1)?,
        mul(a, add(s, 1)?)?,
    ])?;
    let reduction = sum([
        mul(2, r)?,
        t,
        mul(e, sum([d, r, 3])?)?,
        h,
        mul(h, per_group)?,
        mul(t, a)?,
        de,
        mul(2, e)?,
        s,
        mul(k, e1)?,
        r,
        mul(r, t)?,
    ])?;
    if bytes > limits.bytes || inspection > limits.visits || reduction > limits.visits {
        return Err(ContractError::Capacity);
    }
    Ok(ProjectionQuote {
        shape,
        bytes,
        inspection,
        reduction,
    })
}

#[cfg(test)]
mod arithmetic_tests {
    use super::*;

    #[test]
    fn triangular_zero_and_representable_halved_products_do_not_overflow() {
        for (n, expected) in [(0, 0), (1, 0), (2, 1), (3, 3), (10, 45)] {
            assert_eq!(triangular(n).unwrap(), expected);
        }
        let n = (1usize << (usize::BITS / 2)) + 1;
        assert!(n.checked_mul(n - 1).is_none());
        assert_eq!(triangular(n).unwrap(), n * ((n - 1) / 2));
        assert_eq!(triangular(usize::MAX), Err(ContractError::Capacity));
    }
}
