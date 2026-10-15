//! Small continuation values from a candidate's own rows. These are provisional until
//! that exact candidate's outcome commits; they never stand in for a durable receipt.
use super::*;
use focal_model::ClaimStatus;

/// The state one candidate wrote that a subsequent operation can bind to without a read.
/// Fixed slots bound the value to one claim and one response, with no payload hydration
/// or retained heap allocation. Larger mutations simply provide no continuation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeWrittenState {
    pub sequence: SessionSeq,
    pub claim: Option<NativeWrittenClaim>,
    pub response: Option<NativeWrittenResponse>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeWrittenClaim {
    pub binding: Binding,
    pub issuer: ParticipantId,
    pub subject: ParticipantId,
    pub status: ClaimStatus,
    pub receipt: Option<ReceiptEntitlement>,
    pub response_count: u32,
    pub latest_response: Option<TestamentId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeWrittenResponse {
    pub binding: Binding,
    pub claim: ClaimId,
    pub cycle: u32,
}

impl NativePrepared {
    /// Project only this candidate's own puts, at its outcome's sequence. Nothing is
    /// fetched from the later committed root or another speculative candidate.
    pub fn written_state(&self) -> Option<NativeWrittenState> {
        let mut written = NativeWrittenState {
            sequence: self.outcome.sequence,
            claim: None,
            response: None,
        };
        for (_, _, change) in self.fragments.changes() {
            let ranges::RowChange::Put(entry) = change else {
                continue;
            };
            match (entry.key, entry.value) {
                (Key::Claim(_), Row::Claim(claim)) => {
                    if written.claim.is_some() {
                        return None;
                    }
                    let claim = claim.claim()?;
                    written.claim = Some(NativeWrittenClaim {
                        binding: claim.binding(),
                        issuer: claim.issuer(),
                        subject: claim.subject(),
                        status: claim.status(),
                        receipt: claim.receipt(),
                        response_count: u32::try_from(claim.response_count()).ok()?,
                        latest_response: claim.latest_response().map(|link| link.testament),
                    });
                }
                (Key::Response(_), Row::Response(response)) => {
                    if written.response.is_some() {
                        return None;
                    }
                    let identity = response.get()?.identity();
                    written.response = Some(NativeWrittenResponse {
                        binding: identity.binding,
                        claim: identity.claim,
                        cycle: identity.cycle,
                    });
                }
                _ => {}
            }
        }
        (written.claim.is_some() || written.response.is_some()).then_some(written)
    }
}
