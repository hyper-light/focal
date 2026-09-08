//! Authored response construction before owner admission. Borrowed plans price
//! their final buffers without allocating or granting response authority.
use super::prepare::{add, array, within};
use super::{NativeError, NativeLimits};
use focal_model::lifecycle::evidence::SlotBinding;
use focal_model::{ArtifactRef, Confidence, OutcomeKind};

#[path = "response_source.rs"]
mod source;
pub use source::{
    NativeMonitorSource, NativeMonitorSourcePlan, NativeResponseSource, NativeResponseSourcePlan,
    NativeSourceQuote,
};

#[cfg(test)]
#[path = "response_input_tests.rs"]
mod tests;

/// Borrowed authored report. Order, diagnostics and outcome remain exactly as
/// supplied; the owner separately checks evidence membership and authority.
#[derive(Debug, Clone, Copy)]
pub struct NativeResponseSpec<'a> {
    pub summary: &'a str,
    pub confidence: Confidence,
    pub outcome: OutcomeKind,
    pub manifest: &'a [SlotBinding],
    pub diagnostics: &'a [ArtifactRef],
}

/// Typed authored report. There is no default outcome or implicit success.
#[derive(Debug)]
pub struct NativeResponseInput {
    pub summary: String,
    pub confidence: Confidence,
    pub outcome: OutcomeKind,
    pub manifest: Vec<SlotBinding>,
    pub diagnostics: Vec<ArtifactRef>,
}

/// Immutable construction quote, borrowing the exact input it will build.
/// The caller reserves memory before consuming this plan; this type owns none.
#[derive(Debug)]
pub struct NativeResponsePlan<'a> {
    spec: NativeResponseSpec<'a>,
    bytes: usize,
    maximum: usize,
}

fn charge(summary: usize, manifest: usize, diagnostics: usize) -> Result<usize, NativeError> {
    add(
        array::<u8>(summary)?,
        add(
            array::<SlotBinding>(manifest)?,
            array::<ArtifactRef>(diagnostics)?,
        )?,
    )
}

impl NativeResponseSpec<'_> {
    fn check_dimensions(&self, limits: NativeLimits) -> Result<(), NativeError> {
        if self.summary.len() > limits.response_summary_bytes
            || self.manifest.len() > super::response_budget::work_limit(limits)?
            || self.diagnostics.len() > limits.diagnostics_per_cycle
        {
            return Err(NativeError::Capacity("authored response"));
        }
        Ok(())
    }

    /// Shared native semantic intent preimage. This is not a wire encoding.
    pub(super) fn hash_into(&self, hash: &mut blake3::Hasher) -> Result<(), NativeError> {
        source::hash_into(self, hash)
    }
}

impl NativeResponseInput {
    /// Check existing input dimensions and price compact final buffers without
    /// allocating. This does not close a cycle or validate supplied evidence.
    pub fn prepare(
        spec: NativeResponseSpec<'_>,
        limits: NativeLimits,
    ) -> Result<NativeResponsePlan<'_>, NativeError> {
        spec.check_dimensions(limits)?;
        let bytes = charge(
            spec.summary.len(),
            spec.manifest.len(),
            spec.diagnostics.len(),
        )?;
        within(bytes, limits.preparation_bytes)?;
        Ok(NativeResponsePlan {
            spec,
            bytes,
            maximum: limits.preparation_bytes,
        })
    }

    pub fn as_spec(&self) -> NativeResponseSpec<'_> {
        NativeResponseSpec {
            summary: &self.summary,
            confidence: self.confidence,
            outcome: self.outcome,
            manifest: &self.manifest,
            diagnostics: &self.diagnostics,
        }
    }

    pub(super) fn heap_charge(&self) -> Result<usize, NativeError> {
        charge(
            self.summary.capacity(),
            self.manifest.capacity(),
            self.diagnostics.capacity(),
        )
    }

    pub(super) fn check_limits(&self, limits: NativeLimits) -> Result<(), NativeError> {
        self.as_spec().check_dimensions(limits)?;
        within(self.heap_charge()?, limits.preparation_bytes)
    }

    pub(super) fn hash_into(&self, hash: &mut blake3::Hasher) -> Result<(), NativeError> {
        self.as_spec().hash_into(hash)
    }
}

impl<'a> NativeResponsePlan<'a> {
    pub fn spec(&self) -> NativeResponseSpec<'a> {
        self.spec
    }

    /// Final dynamic bytes plus native allocator bookkeeping. The inline input
    /// remains on the caller's stack, as in existing native input accounting.
    pub fn construction_bytes(&self) -> usize {
        self.bytes
    }

    /// Construct under the caller's reserved byte allowance. Actual capacities
    /// are reconciled before retaining each buffer; refusal drops partial work.
    pub fn build(self, max_bytes: usize) -> Result<NativeResponseInput, NativeError> {
        let maximum = max_bytes.min(self.maximum);
        within(self.bytes, maximum)?;
        source::build_slice(self.spec, maximum)
    }
}
