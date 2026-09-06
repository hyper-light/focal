//! Read-side preparation for revision-fenced participant lifecycle commands.
use crate::*;
use focal_model::*;
use focal_wire::*;

impl<T: ClientTransport> Client<T> {
    /// Resolve a revision before preparing a new immutable command. Callers must
    /// reuse their saved expanded command on retry, not refresh this fence.
    pub async fn claim_revision(
        &self,
        ledger: LedgerId,
        claim: ClaimId,
        nonce: RequestId,
    ) -> Result<Option<ObjectRevision>, ClientError> {
        let mut hash = blake3::Hasher::new_derive_key("focal.client.participant-revision.v1");
        hash.update(&nonce.0);
        hash.update(&claim.0);
        let mut id = [0; 16];
        id.copy_from_slice(
            hash.finalize()
                .as_bytes()
                .get(..16)
                .ok_or(ClientError::Configuration)?,
        );
        if id == [0; 16] || claim.is_zero() {
            return Err(ClientError::Configuration);
        }
        let mut references = Vec::new();
        references
            .try_reserve_exact(1)
            .map_err(|_| ClientError::Access(AccessError::Capacity))?;
        references.push(ObjectRef::claim(ledger, claim));
        let page = self
            .read(RequestEnvelope {
                protocol: PROTOCOL_VERSION,
                ledger,
                route_epoch: RouteEpoch(1),
                request_epoch: RequestEpoch(1),
                request_id: RequestId(id),
                operation: Operation::Read(ReadRequest {
                    consistency: ReadConsistency::Linearizable,
                    query: ReadQuery::Objects(references),
                    max_items: 1,
                }),
            })
            .await?;
        if page.next.is_some() || page.objects.len() > 1 {
            return Err(ClientError::InvalidResponse);
        }
        match page.objects.into_iter().next() {
            None => Ok(None),
            Some(ReadObject::Claim { id, value }) if id == claim => {
                Ok(Some(value.lifecycle().revision))
            }
            _ => Err(ClientError::InvalidResponse),
        }
    }
}
