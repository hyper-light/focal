//! A spilled claim row read back (doc 31 M3): its record encoding, as a record and a checkpoint
//! carry it, decoded and checked as a replayed one is, the declarations, responses and
//! evaluations it names read from the rows of the view it was read in.
use super::{bytes::Cursor, read_claim, read_evidence as evidence, read_source::Meter};
use crate::native::{EvaluationKey, LedgerId, NativeError, NativeLimits, OwnedClaim, Row};
use focal_memory::Allocation;
use focal_model::lifecycle::{ContractError, aggregation, evidence::Response, validation};
use focal_model::{ClaimId, TestamentId, ValidationId};

/// The rows a spilled claim's encoding names, as the view it is read in holds them. None of
/// them is a claim, so reading one never reads a claim back.
pub(in crate::native) trait ClaimRows {
    fn declaration(&self, id: ValidationId) -> Option<&validation::Declaration>;
    fn response(&self, id: TestamentId) -> Option<&Response>;
    fn evaluation(&self, key: EvaluationKey) -> Option<&validation::EvaluationState>;
}

struct Rows<'r, R: ?Sized>(&'r R);
impl<R: ClaimRows + ?Sized> read_claim::Objects for Rows<'_, R> {
    fn declaration(&self, id: ValidationId) -> Result<&validation::Declaration, ContractError> {
        self.0.declaration(id).ok_or(ContractError::InvalidManifest)
    }
    fn response(&self, id: TestamentId) -> Result<&Response, ContractError> {
        self.0.response(id).ok_or(ContractError::InvalidManifest)
    }
    fn evaluation(
        &self,
        key: EvaluationKey,
    ) -> Result<&validation::EvaluationState, ContractError> {
        self.0.evaluation(key).ok_or(ContractError::InvalidManifest)
    }
}

/// Codec and model work a spilled row's decode may take a byte of its encoding, and beside its
/// bytes: the bytes are the replica's own, written once its checks had read the row, and at most
/// `payloads::SPILLED_BYTES` long, so the work stays bounded by their length.
const WORK_PER_BYTE: usize = 256;
const WORK_FIXED: usize = 65_536;

/// The claim row `id` whose record encoding `bytes` are, built as a replayed one is under `native`
/// and its heap charged by `fund` at its quote before it is built. Every count the encoding holds
/// takes a byte of it at least, so the acceptance bounds are its length.
pub(in crate::native) fn decode_claim<R: ClaimRows + ?Sized>(
    id: ClaimId,
    bytes: &[u8],
    ledger: LedgerId,
    rows: &R,
    native: NativeLimits,
    fund: &dyn Fn(usize) -> Result<Allocation, NativeError>,
) -> Result<(OwnedClaim, Allocation), NativeError> {
    let len = bytes.len();
    let work = len
        .checked_mul(WORK_PER_BYTE)
        .and_then(|work| work.checked_add(WORK_FIXED))
        .ok_or(NativeError::Capacity("claim read-back work"))?;
    let mut cursor = Cursor::new(bytes, len, work).map_err(evidence::codec)?;
    let input = read_claim::Input::read(&mut cursor).map_err(evidence::codec)?;
    cursor.finish().map_err(evidence::codec)?;
    if input.fields.binding.ledger != ledger || input.fields.binding.object.0 != id.0 {
        return Err(ContractError::InvalidManifest.into());
    }
    let limits = read_claim::Limits {
        native,
        acceptance: aggregation::Limits {
            max_slots: len,
            max_checks: len,
            max_results: len,
            max_updates: len,
        },
    };
    let (source, model) = (Meter::new(work), Meter::new(work));
    let objects = Rows(rows);
    let quote = input.quote(&objects, limits, &source, &model)?;
    let charge = fund(quote.heap_bytes)?;
    match input.build(&objects, limits, &source, &model, quote.heap_bytes)? {
        (Row::Claim(owned), actual) if actual <= charge.bytes() => Ok((owned, charge)),
        _ => Err(ContractError::InvalidManifest.into()),
    }
}
