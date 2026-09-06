//! Coherent observed validation context, assembled with at most three read calls.
//! This is not an assignment, execution lease, or proof of a single artifact
//! target. Older runs retain their original target/manifest/attempt identities.
use crate::{Client, ClientError, ClientTransport};
use focal_model::*;
use focal_wire::*;
use serde::{Deserialize, Serialize};
use std::{
    future::{Future, poll_fn},
    io::{self, Write},
    panic::{AssertUnwindSafe, catch_unwind},
    pin::pin,
    task::Poll,
    time::Duration,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamedTestament {
    pub id: TestamentId,
    pub value: Testament,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidationContext {
    pub token: ReadToken,
    pub validation_id: ValidationId,
    pub validation: Validation,
    pub claim: Claim,
    /// The claim's current closing response, whose receipt may precede a later
    /// adoption. No historical run is relabeled as targeting this testament.
    pub testament: Option<NamedTestament>,
    pub records: Vec<ValidationResult>,
    /// Pass unchanged to ValidationResults with Exact(self.token).
    pub next: Option<ValidationResultPosition>,
}

#[derive(Debug, thiserror::Error)]
pub enum ValidationContextError {
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error("validation requirement was not found")]
    NotFound,
    #[error("validation context requires a linearizable or exact validation-results read")]
    InvalidRequest,
    #[error("validation context exceeds the bounded aggregate output allowance")]
    Capacity,
}

type Result<T> = std::result::Result<T, ValidationContextError>;
fn invalid() -> ValidationContextError {
    ClientError::InvalidResponse.into()
}

impl<T: ClientTransport> Client<T> {
    /// Read the requirement and one run page, then its parent and optional
    /// current testament at exactly that same token. Expiry or route failure is
    /// returned without restarting at a fresh prefix. No mutation ordinal or
    /// journal is allocated. Transport retries retain each read's identity.
    ///
    /// Combined JSON output is bounded by the client's maximum frame size. The
    /// whole call is bounded by min(RetryPolicy::max_elapsed, 30 seconds), so its
    /// separate reads cannot each consume the full retry deadline.
    pub async fn validation_context(
        &self,
        mut request: RequestEnvelope,
    ) -> Result<ValidationContext> {
        let (id, exact) = validate_input(&request, self.wire_limits())?;
        if let Some(token) = exact {
            request.route_epoch = token.route_epoch;
        }
        let duration = self.retry_timeout().min(Duration::from_secs(30));
        // Tokio's timer creation/poll can unwind without a time driver. Keep
        // that dependency failure inside this read-only interface and drop the
        // in-flight wait and all retained response ownership on failure.
        let future = async {
            tokio::time::timeout(duration, self.context_inner(request, id, exact))
                .await
                .map_err(|_| ClientError::Transport)?
        };
        let mut future = pin!(future);
        poll_fn(
            |cx| match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
                Ok(result) => result,
                Err(_) => Poll::Ready(Err(ClientError::Transport.into())),
            },
        )
        .await
    }

    async fn context_inner(
        &self,
        request: RequestEnvelope,
        id: ValidationId,
        exact: Option<ReadToken>,
    ) -> Result<ValidationContext> {
        let mut allowance = JsonAllowance::new(self.wire_limits().max_frame_bytes as usize);
        let page = self.read(request.clone()).await?;
        allowance.add(&page)?;
        let token = page.token;
        if token.ledger != request.ledger
            || token.route_epoch.0 == 0
            || exact.is_some_and(|expected| expected != token)
            || page.next.is_some()
            || page.objects.len() > 1
        {
            return Err(invalid());
        }
        let Some(object) = page.objects.into_iter().next() else {
            return Err(ValidationContextError::NotFound);
        };
        let ReadObject::ValidationResults {
            id: actual,
            value: validation,
            records,
            next,
        } = object
        else {
            return Err(invalid());
        };
        if actual != id
            || validation.content().claim.is_zero()
            || validation.content().schema != SCHEMA_MAJOR
            || validation.content().ledger != token.ledger
            || validation.lifecycle().created > token.sequence
            || validation
                .content()
                .content_hash()
                .map_err(|_| ValidationContextError::Capacity)?
                != validation.content_hash()
        {
            return Err(invalid());
        }
        let claim_id = validation.content().claim;
        let parent = self
            .read(followup(
                &request,
                token,
                ObjectRef::claim(token.ledger, claim_id),
            )?)
            .await?;
        allowance.add(&parent)?;
        if parent.token != token || parent.next.is_some() || parent.objects.len() != 1 {
            return Err(invalid());
        }
        let Some(ReadObject::Claim {
            id: actual,
            value: claim,
        }) = parent.objects.into_iter().next()
        else {
            return Err(invalid());
        };
        if actual != claim_id
            || claim.content().schema != SCHEMA_MAJOR
            || claim.content().ledger != token.ledger
            || claim.lifecycle().created > token.sequence
            || claim
                .lifecycle()
                .history
                .iter()
                .any(|fact| fact.sequence > token.sequence)
            || claim
                .lifecycle()
                .receipt
                .as_ref()
                .is_some_and(|receipt| receipt.acquired > token.sequence)
            || claim
                .content()
                .content_hash()
                .map_err(|_| ValidationContextError::Capacity)?
                != claim.content_hash()
        {
            return Err(invalid());
        }
        let specification = validation
            .content()
            .specification_hash()
            .map_err(|_| ValidationContextError::Capacity)?;
        let mut membership = claim
            .content()
            .requirements
            .iter()
            .filter(|requirement| requirement.id == id);
        if membership
            .next()
            .is_none_or(|requirement| requirement.specification != specification)
            || membership.next().is_some()
        {
            return Err(invalid());
        }
        let testament = if let Some(testament_id) = claim.lifecycle().testament {
            let reference = ObjectRef {
                ledger: token.ledger,
                kind: ObjectKind::Testament,
                id: ObjectId(testament_id.0),
            };
            let page = self.read(followup(&request, token, reference)?).await?;
            allowance.add(&page)?;
            if page.token != token || page.next.is_some() || page.objects.len() != 1 {
                return Err(invalid());
            }
            let Some(ReadObject::Testament { id: actual, value }) = page.objects.into_iter().next()
            else {
                return Err(invalid());
            };
            if actual != testament_id
                || value.content().schema != SCHEMA_MAJOR
                || value.content().claim != claim_id
                || value.content().ledger != token.ledger
                || Some(value.content().evidence_set) != claim.lifecycle().evidence_set
                || value.lifecycle().created > token.sequence
                || value
                    .lifecycle()
                    .acknowledged
                    .is_some_and(|at| at < value.lifecycle().created || at > token.sequence)
                || value
                    .content()
                    .content_hash()
                    .map_err(|_| ValidationContextError::Capacity)?
                    != value.content_hash()
            {
                return Err(invalid());
            }
            Some(NamedTestament {
                id: testament_id,
                value,
            })
        } else {
            None
        };
        let context = ValidationContext {
            token,
            validation_id: id,
            validation,
            claim,
            testament,
            records,
            next,
        };
        JsonAllowance::new(self.wire_limits().max_frame_bytes as usize).add(&context)?;
        Ok(context)
    }
}

fn validate_input(
    request: &RequestEnvelope,
    limits: &WireLimits,
) -> Result<(ValidationId, Option<ReadToken>)> {
    let Operation::Read(ReadRequest {
        consistency,
        query: ReadQuery::ValidationResults { id, after },
        max_items,
    }) = &request.operation
    else {
        return Err(ValidationContextError::InvalidRequest);
    };
    if !matches!(
        request.protocol,
        PROTOCOL_VERSION | MANAGED_PROTOCOL_VERSION
    ) || request.ledger.tenant.is_zero()
        || request.ledger.session.is_zero()
        || request.request_id.is_zero()
        || request.request_epoch.0 == 0
        || request.route_epoch.0 == 0
        || id.is_zero()
        || *max_items == 0
        || *max_items > limits.max_items
        || after.is_some_and(|position| position.run.validation != *id || position.run.epoch == 0)
    {
        return Err(ValidationContextError::InvalidRequest);
    }
    let exact = match consistency {
        ReadConsistency::Linearizable if after.is_none() => None,
        ReadConsistency::Exact(token)
            if token.ledger == request.ledger && token.route_epoch.0 > 0 =>
        {
            Some(*token)
        }
        _ => return Err(ValidationContextError::InvalidRequest),
    };
    Ok((*id, exact))
}

fn followup(
    original: &RequestEnvelope,
    token: ReadToken,
    reference: ObjectRef,
) -> Result<RequestEnvelope> {
    let mut hasher = blake3::Hasher::new_derive_key("focal.client.validation-context.read.v1");
    hasher.update(&original.request_id.0);
    hasher.update(&original.request_epoch.0.to_be_bytes());
    hasher.update(&token.ledger.tenant.0);
    hasher.update(&token.ledger.session.0);
    hasher.update(&token.sequence.0.to_be_bytes());
    hasher.update(&token.route_epoch.0.to_be_bytes());
    hasher.update(&reference.kind.code().to_be_bytes());
    hasher.update(&reference.id.0);
    let mut bytes = [0; 16];
    let digest = hasher.finalize();
    bytes.copy_from_slice(
        digest
            .as_bytes()
            .get(..16)
            .ok_or(ValidationContextError::Capacity)?,
    );
    let request_id = RequestId(bytes);
    if request_id.is_zero() || request_id == original.request_id {
        return Err(ValidationContextError::InvalidRequest);
    }
    let mut references = Vec::new();
    references
        .try_reserve_exact(1)
        .map_err(|_| ValidationContextError::Capacity)?;
    references.push(reference);
    Ok(RequestEnvelope {
        protocol: original.protocol,
        ledger: token.ledger,
        route_epoch: token.route_epoch,
        request_epoch: original.request_epoch,
        request_id,
        operation: Operation::Read(ReadRequest {
            consistency: ReadConsistency::Exact(token),
            query: ReadQuery::Objects(references),
            max_items: 1,
        }),
    })
}

struct JsonAllowance {
    used: usize,
    limit: usize,
}
impl JsonAllowance {
    fn new(limit: usize) -> Self {
        Self { used: 0, limit }
    }
    fn add(&mut self, value: &impl Serialize) -> Result<()> {
        serde_json::to_writer(self, value).map_err(|_| ValidationContextError::Capacity)
    }
}
impl Write for JsonAllowance {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let used = self
            .used
            .checked_add(bytes.len())
            .filter(|used| *used <= self.limit)
            .ok_or_else(|| io::Error::other("validation context output capacity"))?;
        self.used = used;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
