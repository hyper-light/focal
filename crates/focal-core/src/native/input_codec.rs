//! Dormant native input representation, independent of V1 and persisted rows.
//!
//! These bytes are not a registered transport or a durable successor promise.
//! Complete semantic decoding, owner-selected funding, recorded-mutation replay
//! and activation are separate gates. Encoding neither grants actor/timer
//! authority nor authenticates a request, verifies custody or launches execution.
mod admission;
pub(in crate::native) mod artifact;
mod artifact_fields;
mod authored_creation;
pub(in crate::native) mod bytes;
mod claim_source;
pub(in crate::native) mod creation_content;
pub(in crate::native) mod descriptors;
mod encode;
mod fixed;
mod ingress;
mod inspect;
mod legacy_creation;
mod response;
mod source_bytes;
pub(in crate::native) mod types;
mod validation_source;

#[cfg(test)]
mod frame_tests;

use super::*;
pub use admission::DecodedRequest;
pub(in crate::native) use admission::Plan as DecodedPlan;
pub use artifact::{ArtifactFramePlan, ArtifactInputQuote, ArtifactInputView};
pub use authored_creation::{
    AuthoredCreationLimits, AuthoredCreationQuote, AuthoredCreationWork, AuthoredFramePlan,
};
pub use bytes::Error as CodecError;
use bytes::{CountingSink, SliceSink};
pub use creation_content::{
    BodyConstructionQuote, BodyInspectionLimits, BodyParseQuote, ClaimBodyInput, ClaimBodyPlan,
    DeclarationBodyInput, DeclarationBodyPlan, ValidationBodyInput, ValidationBodyPlan,
};
pub use fixed::FixedFrame;
pub use ingress::{DecodeQuote, DecodeWork, NativeDecodeLimits};
pub use inspect::{FrameKind, InputHeader, InspectionLimits, InspectionQuote, StructuralInput};
pub use legacy_creation::{
    LegacyCreationLimits, LegacyCreationPlan, LegacyCreationQuote, LegacyCreationWork,
};
pub use response::{DynamicInputQuote, MonitorFramePlan, ResponseFramePlan};

/// Structural and semantic refusals remain distinct. Neither supplies an
/// admitted outcome or selects an owner reservation.
#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("native input: {0}")]
    Codec(#[from] CodecError),
    #[error(transparent)]
    Native(#[from] NativeError),
}
impl From<ContractError> for DecodeError {
    fn from(error: ContractError) -> Self {
        Self::Native(error.into())
    }
}
impl From<MemoryError> for DecodeError {
    fn from(error: MemoryError) -> Self {
        Self::Native(error.into())
    }
}

/// A separate magic prevents accidental V1 or checkpoint interpretation.
pub const MAGIC: [u8; 8] = *b"FCNINPUT";
pub const VERSION: u16 = 1;

#[derive(Debug, Clone, Copy)]
pub struct EncodingLimits {
    pub bytes: usize,
    pub visits: usize,
}

/// Source objects remain borrowed for the entire measure/write operation.
/// Timer delivery context (including actual firing time) is never encoded here.
#[derive(Debug, Clone, Copy)]
pub enum InputFrame<'a> {
    Request {
        ledger: LedgerId,
        profile: NativeContentProfile,
        input: &'a NativeInput,
    },
    EvaluationDeadline {
        ledger: LedgerId,
        profile: NativeContentProfile,
        input: NativeDeadlineInput,
    },
    ClaimDeadline {
        ledger: LedgerId,
        profile: NativeContentProfile,
        input: NativeClaimDeadlineInput,
    },
    MonitorDeadline {
        ledger: LedgerId,
        profile: NativeContentProfile,
        input: NativeMonitorDeadlineInput,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EncodingQuote {
    pub bytes: usize,
    pub visits: usize,
}

/// A complete successful dry run binds exact bytes/work to this immutable source.
/// The caller owns and funds the eventual output buffer; no Vec or Arc is created.
#[derive(Debug)]
pub struct EncodingPlan<'a> {
    source: InputFrame<'a>,
    quote: EncodingQuote,
}
impl<'a> EncodingPlan<'a> {
    pub fn prepare(source: InputFrame<'a>, limits: EncodingLimits) -> Result<Self, CodecError> {
        let mut sink = CountingSink::new(limits.bytes, limits.visits);
        encode::frame(&mut sink, source)?;
        Ok(Self {
            source,
            quote: EncodingQuote {
                bytes: sink.len(),
                visits: sink.visits_used(),
            },
        })
    }
    pub fn quote(&self) -> EncodingQuote {
        self.quote
    }
    /// Wrong-sized destinations refuse before any byte is changed. All field
    /// widths and resource bounds were checked against the same borrowed source.
    pub fn write_into(&self, output: &mut [u8]) -> Result<(), CodecError> {
        if output.len() != self.quote.bytes {
            return Err(CodecError::Capacity);
        }
        let mut sink = SliceSink::new(output, self.quote.visits);
        encode::frame(&mut sink, self.source)?;
        if sink.len() != self.quote.bytes || sink.visits_used() != self.quote.visits {
            return Err(CodecError::Capacity);
        }
        sink.finish()
    }
}
