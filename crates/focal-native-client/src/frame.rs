//! Frame bytes and their owner-side intent fingerprint.
use crate::CompileError;
use focal_core::native::{
    NativeContentProfile, NativeInput, NativeLimits,
    input_codec::{DecodeWork, EncodingLimits, EncodingPlan, InputFrame, NativeDecodeLimits},
};
use focal_model::{ContentHash, LedgerId};

/// Bounds for one encoded request frame and the local dry-run decode that
/// computes its fingerprint. The default frame bound equals the ordinary actor
/// wire capability; larger frames need an explicit journal format extension.
#[derive(Debug, Clone, Copy)]
pub struct FrameLimits {
    pub bytes: usize,
    pub visits: usize,
    pub decode_work: usize,
}
impl Default for FrameLimits {
    fn default() -> Self {
        Self {
            bytes: 1024 * 1024,
            visits: 1 << 24,
            decode_work: 1 << 24,
        }
    }
}

/// Encode one request frame. The returned bytes are the journaled wire body:
/// the client resends exactly these bytes on every retry.
pub fn encode_frame(
    ledger: LedgerId,
    profile: NativeContentProfile,
    input: &NativeInput,
    limits: EncodingLimits,
) -> Result<Vec<u8>, CompileError> {
    let plan = EncodingPlan::prepare(
        InputFrame::Request {
            ledger,
            profile,
            input,
        },
        limits,
    )?;
    let bytes = plan.quote().bytes;
    let mut frame = Vec::new();
    frame
        .try_reserve_exact(bytes)
        .map_err(|_| CompileError::Capacity("frame buffer"))?;
    frame.resize(bytes, 0);
    plan.write_into(&mut frame)?;
    Ok(frame)
}

/// The owner's semantic intent fingerprint of a frame, computed by the same
/// decoder the owner runs. A committed receipt carries this value, which binds
/// the receipt to the journaled bytes without trusting the reply's own claim.
pub fn fingerprint(
    frame: &[u8],
    native: NativeLimits,
    limits: FrameLimits,
) -> Result<ContentHash, CompileError> {
    let work = DecodeWork {
        parse: limits.decode_work,
        source: limits.decode_work,
        model: limits.decode_work,
        acceptance: limits.decode_work,
        native: limits.decode_work,
    };
    let decode = NativeDecodeLimits::for_native(native, limits.bytes, work)?;
    Ok(decode.with_request(native, frame, |decoded, _| decoded.intent_fingerprint())?)
}
