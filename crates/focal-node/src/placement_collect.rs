//! Assemble a voter-majority proof of one session fact. Signatures over one
//! identical statement merge into one proof; a differing statement is a
//! different fact and is dropped. Collection stops at the first majority.
use crate::placement_control::{SessionFact, SessionSignReply, SessionSignRequest};
use crate::placement_proof::{PlacementProofError, ProofWindow};
use focal_directory::AuthorityProof;
use focal_model::{LedgerId, RequestEpoch, RequestId, RouteEpoch};
use focal_wire::{Operation, PROTOCOL_VERSION, PeerConnectionPool, RequestEnvelope};
use std::time::Duration;

const REMOTE_TIMEOUT: Duration = Duration::from_secs(5);
/// Voters are bounded by the directory's member limit.
pub const MAX_VOTERS: usize = 127;

#[derive(Debug, thiserror::Error)]
pub enum CollectError {
    #[error("no voter majority signed the session fact")]
    Quorum,
    #[error("session fact collection exceeds its bounded allowance")]
    Capacity,
    #[error("local proof: {0}")]
    Proof(#[from] PlacementProofError),
}

/// Signatures gathered so far over one statement.
pub struct Collected {
    proof: Option<AuthorityProof>,
    needed: usize,
}
impl Collected {
    pub fn new(voters: usize) -> Result<Self, CollectError> {
        if voters == 0 || voters > MAX_VOTERS {
            return Err(CollectError::Capacity);
        }
        Ok(Self {
            proof: None,
            needed: voters.saturating_div(2).saturating_add(1),
        })
    }
    /// Merge one node's proof; returns whether a majority now agrees.
    pub fn merge(&mut self, incoming: AuthorityProof) -> Result<bool, CollectError> {
        match &mut self.proof {
            None => {
                self.proof = Some(incoming);
            }
            Some(existing) if existing.statement == incoming.statement => {
                for signature in incoming.signatures {
                    if existing
                        .signatures
                        .iter()
                        .any(|known| known.certificate == signature.certificate)
                    {
                        continue;
                    }
                    existing
                        .signatures
                        .try_reserve_exact(1)
                        .map_err(|_| CollectError::Capacity)?;
                    existing.signatures.push(signature);
                }
            }
            Some(_) => {}
        }
        Ok(self.complete())
    }
    pub fn complete(&self) -> bool {
        self.proof
            .as_ref()
            .is_some_and(|proof| proof.signatures.len() >= self.needed)
    }
    pub fn finish(self) -> Result<AuthorityProof, CollectError> {
        match self.proof {
            Some(proof) if proof.signatures.len() >= self.needed => Ok(proof),
            _ => Err(CollectError::Quorum),
        }
    }
}

/// Serialize the signature request body once. Every voter in a collection round
/// signs the identical (fact, window), so the caller builds this once and hands
/// each `remote_signature` the shared bytes instead of re-serializing per voter.
pub fn sign_request_body(fact: &SessionFact, window: ProofWindow) -> Option<Vec<u8>> {
    postcard::to_stdvec(&SessionSignRequest {
        schema: 1,
        fact: fact.clone(),
        window,
    })
    .ok()
}
/// Ask one voter for its signature. An unreachable or refusing voter yields
/// None; the majority decides, never one node.
pub async fn remote_signature(
    pool: &PeerConnectionPool,
    voter: u64,
    ledger: LedgerId,
    group: [u8; 16],
    body: &[u8],
    request_id: RequestId,
) -> Option<AuthorityProof> {
    // The signed body (fact + window) is identical for every voter, so the
    // caller serializes it once; each voter's envelope only copies those bytes.
    let envelope = RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger,
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id,
        operation: Operation::SessionSign {
            group,
            request: body.to_vec(),
        },
    };
    let bytes = tokio::time::timeout(REMOTE_TIMEOUT, pool.send_placement(voter, &envelope))
        .await
        .ok()?
        .ok()?;
    match postcard::from_bytes::<SessionSignReply>(&bytes).ok()? {
        SessionSignReply::Signed(proof) if proof.signatures.len() == 1 => Some(*proof),
        _ => None,
    }
}
