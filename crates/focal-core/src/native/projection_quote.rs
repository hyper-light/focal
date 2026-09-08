//! Read-only future-work estimates for an embedding's native owner allowance.
//! Quotes do not hold memory, pin a cohort, or authorize an evaluator to begin.
use super::*;
use focal_model::lifecycle::aggregation::{
    self, ProjectionLimits, ProjectionQuote, ProjectionShape,
};

/// The model peak plus the complete native adapter lookup allowance for the
/// promised shape. Source-owned rows, range-tree descent and comparison work
/// inside model helpers/sorts remain separate from these counted visits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeProjectionQuote {
    model: ProjectionQuote,
    lookup_visits: usize,
    overlay_rows: usize,
    retired_cycles: usize,
}
impl NativeProjectionQuote {
    /// Use the actual owner's immutable limits and preserve these inclusive
    /// future maxima before publication of later growth. This function allocates
    /// nothing; it is an estimate for admission planning, never a reservation.
    pub fn derive(
        limits: NativeLimits,
        claim: &ClaimState,
        shape: ProjectionShape,
        overlay_rows: usize,
    ) -> Result<Self, NativeError> {
        Self::derive_with_retired(limits, claim, shape, overlay_rows, 0)
    }

    /// Retained diagnostic-only cycles also cost lookups, even though they add
    /// no work to the model shape. Callers must include retired work in `shape`
    /// and preserve both maxima before later publication.
    pub fn derive_with_retired(
        limits: NativeLimits,
        claim: &ClaimState,
        shape: ProjectionShape,
        overlay_rows: usize,
        retired_cycles: usize,
    ) -> Result<Self, NativeError> {
        super::prepare::within(overlay_rows, limits.range.max_batch_entries)?;
        super::prepare::within(retired_cycles, limits.plan_edges)?;
        let model = aggregation::quote_projection(
            claim,
            shape,
            ProjectionLimits {
                responses: limits.responses,
                works: limits.plan_edges,
                slots: limits.plan_edges,
                evaluations: limits.evaluations_per_claim,
                declarations: limits.definitions,
                visits: limits.plan_edges,
                bytes: limits.preparation_bytes,
            },
        )?;
        let r = shape.responses;
        let w = shape.works;
        let e = shape.evaluations;
        let t = mul(r, claim.acceptance().slot_count())?;
        let d = claim.acceptance().declarations().len();
        // Every source/reducer direct lookup, excluding membership cursors.
        // A publication in the original entry gates is Admission/Increment;
        // neither adds another response lookup beyond the gate's own scan.
        let direct = sum([
            d,
            mul(9, e)?,
            mul(3, r)?,
            triangular(r)?,
            mul(2, t)?,
            mul(2, triangular(e)?)?,
            mul(3, mul(r, e)?)?,
            w,
            mul(w, r)?,
        ])?;
        // Complete indexed-work scans, including constructors of partial and
        // empty duplicate-prefix scans. One per-map work lookup follows each
        // yielded row in addition to the cursor's own source/provenance checks.
        let cursors = sum([t, e, w, 1])?;
        // Header + current open cycle, then each retired link/cycle/receipt
        // check and cycle load. All work-chain walks remain covered by 8*w.
        let per_cursor = sum([3, mul(2, r)?, mul(4, retired_cycles)?, mul(8, w)?])?;
        let lookup_visits = sum([
            2, // Query claim and registration headers; mutation uses at most this.
            mul(cursors, per_cursor)?,
            mul(add(direct, mul(cursors, w)?)?, add(1, overlay_rows)?)?,
        ])?;
        super::prepare::within(lookup_visits, limits.plan_edges)?;
        Ok(Self {
            model,
            lookup_visits,
            overlay_rows,
            retired_cycles,
        })
    }
    pub fn model(&self) -> ProjectionQuote {
        self.model
    }
    pub fn lookup_visits(&self) -> usize {
        self.lookup_visits
    }
    pub fn overlay_rows(&self) -> usize {
        self.overlay_rows
    }
    pub fn retired_cycles(&self) -> usize {
        self.retired_cycles
    }
}
fn add(a: usize, b: usize) -> Result<usize, NativeError> {
    super::prepare::add(a, b)
}
fn mul(a: usize, b: usize) -> Result<usize, NativeError> {
    a.checked_mul(b)
        .ok_or(NativeError::Capacity("projection lookup multiplication"))
}
fn sum<const N: usize>(values: [usize; N]) -> Result<usize, NativeError> {
    values.into_iter().try_fold(0, add)
}
fn triangular(value: usize) -> Result<usize, NativeError> {
    let Some(previous) = value.checked_sub(1) else {
        return Ok(0);
    };
    if value.is_multiple_of(2) {
        mul(
            value.checked_div(2).ok_or(ContractError::Capacity)?,
            previous,
        )
    } else {
        mul(
            value,
            previous.checked_div(2).ok_or(ContractError::Capacity)?,
        )
    }
}
