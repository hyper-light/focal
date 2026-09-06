//! Bounded singular claim selection at one authoritative snapshot.
use crate::{Client, ClientError, ClientTransport};
use focal_model::{CanonicalContent, Claim, ClaimId, ObjectKind, SCHEMA_MAJOR};
use focal_wire::*;
use std::{
    future::{Future, poll_fn},
    panic::{AssertUnwindSafe, catch_unwind},
    pin::pin,
    task::Poll,
    time::Duration,
};

#[derive(Debug, thiserror::Error)]
pub enum ClaimGetError {
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error("claim was not found at the observed prefix")]
    NotFound,
    #[error("claim selector matched more than one claim; narrow the filters or list claims")]
    Ambiguous,
    #[error("claim selection exceeded its page or time budget; uniqueness is unproven")]
    Incomplete,
    #[error("claim selection requires one exact claim ID or a fresh nonempty claim filter")]
    InvalidRequest,
}
impl<T: ClientTransport> Client<T> {
    /// Returns exactly one claim only after proving uniqueness over a single
    /// prefix. Empty filtered pages are progress, not evidence of absence.
    /// No mutation identity is reserved and expiry never starts a fresh scan.
    pub async fn claim_get(&self, mut request: RequestEnvelope) -> Result<ReadPage, ClaimGetError> {
        let exact = match &request.operation {
            Operation::Read(read) => {
                let ReadQuery::Objects(refs) = &read.query else {
                    return Err(ClaimGetError::InvalidRequest);
                };
                if refs.len() != 1
                    || refs.first().is_none_or(|r| {
                        r.kind != ObjectKind::Claim || r.ledger != request.ledger || r.id.is_zero()
                    })
                    || read.max_items != 1
                {
                    return Err(ClaimGetError::InvalidRequest);
                }
                match read.consistency {
                    ReadConsistency::Linearizable => {}
                    ReadConsistency::Exact(token)
                        if token.ledger == request.ledger && token.route_epoch.0 != 0 =>
                    {
                        request.route_epoch = token.route_epoch
                    }
                    _ => return Err(ClaimGetError::InvalidRequest),
                }
                true
            }
            Operation::Select(selection) => {
                if selection.query.filter.kind != ObjectKind::Claim
                    || (selection.predicates.is_empty()
                        && selection.query.filter == ListFilter::new(ObjectKind::Claim))
                    || selection
                        .validate(request.ledger, self.wire_limits())
                        .is_err()
                    || selection.query.cursor.is_some()
                    || selection.query.max_items != 2
                {
                    return Err(ClaimGetError::InvalidRequest);
                }
                false
            }
            Operation::List(list) => {
                if list.filter.kind != ObjectKind::Claim
                    || list.filter == ListFilter::new(ObjectKind::Claim)
                    || list.filter.validate().is_err()
                    || list.cursor.is_some()
                    || list.max_items != 2
                    || list.max_visits == 0
                    || list.max_visits > self.wire_limits().max_items
                {
                    return Err(ClaimGetError::InvalidRequest);
                }
                false
            }
            _ => return Err(ClaimGetError::InvalidRequest),
        };
        let duration = self.retry_timeout().min(Duration::from_secs(30));
        let future = async {
            tokio::time::timeout(duration, self.claim_inner(request, exact))
                .await
                .map_err(|_| ClaimGetError::Incomplete)?
        };
        let mut future = pin!(future);
        poll_fn(
            |cx| match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
                Ok(value) => value,
                Err(_) => Poll::Ready(Err(ClientError::Transport.into())),
            },
        )
        .await
    }
    async fn claim_inner(
        &self,
        mut request: RequestEnvelope,
        exact: bool,
    ) -> Result<ReadPage, ClaimGetError> {
        if exact {
            let page = self.read(request).await?;
            if page.next.is_some() || page.objects.len() > 1 {
                return Err(ClientError::InvalidResponse.into());
            }
            let Some(object) = page.objects.first() else {
                return Err(ClaimGetError::NotFound);
            };
            verify_claim(object, page.token, None)?;
            return Ok(page);
        }
        let mut token = None;
        let mut found = None;
        let seed = request.request_id;
        for page_number in 0u32..64 {
            let reply = self.request(request.clone()).await?;
            let Response::Listed(page) = reply.result else {
                return Err(ClientError::InvalidResponse.into());
            };
            if token.is_some_and(|token| token != page.token)
                || page.token.ledger != request.ledger
                || page.token.route_epoch.0 == 0
                || page.objects.len() > 2
            {
                return Err(ClientError::InvalidResponse.into());
            }
            token = Some(page.token);
            let list =
                selection_request(&request.operation).ok_or(ClaimGetError::InvalidRequest)?;
            for object in page.objects {
                verify_claim(&object, page.token, Some(&list.filter))?;
                if matches!(&request.operation, Operation::Select(selection) if !selection.matches(&object))
                {
                    return Err(ClientError::InvalidResponse.into());
                }
                if found.replace(object).is_some() {
                    return Err(ClaimGetError::Ambiguous);
                }
            }
            let Some(next) = page.next else {
                let object = found.ok_or(ClaimGetError::NotFound)?;
                let mut objects = Vec::new();
                objects
                    .try_reserve_exact(1)
                    .map_err(|_| ClientError::InvalidResponse)?;
                objects.push(object);
                return Ok(ReadPage {
                    token: page.token,
                    objects,
                    next: None,
                });
            };
            let list = selection_request_mut(&mut request.operation)
                .ok_or(ClaimGetError::InvalidRequest)?;
            if list.cursor.as_ref() == Some(&next) {
                return Err(ClientError::InvalidResponse.into());
            }
            list.cursor = Some(next);
            // Keep page limits stable: Select authenticates them in its cursor.
            list.max_items = 2;
            request.route_epoch = page.token.route_epoch;
            let mut hasher = blake3::Hasher::new_derive_key("focal.client.claim-selection-page.v1");
            hasher.update(&seed.0);
            hasher.update(&page_number.to_be_bytes());
            let digest = hasher.finalize();
            request.request_id = focal_model::RequestId(
                digest
                    .as_bytes()
                    .get(..16)
                    .ok_or(ClientError::InvalidResponse)?
                    .try_into()
                    .map_err(|_| ClientError::InvalidResponse)?,
            );
            if request.request_id.is_zero() {
                return Err(ClientError::InvalidResponse.into());
            }
        }
        Err(ClaimGetError::Incomplete)
    }
}
fn verify_claim(
    object: &ReadObject,
    token: ReadToken,
    filter: Option<&ListFilter>,
) -> Result<(), ClaimGetError> {
    let ReadObject::Claim { id, value } = object else {
        return Err(ClientError::InvalidResponse.into());
    };
    if id.is_zero()
        || value.content().schema != SCHEMA_MAJOR
        || value
            .lifecycle()
            .history
            .iter()
            .any(|fact| fact.sequence > token.sequence)
        || value.content().ledger != token.ledger
        || value.lifecycle().created > token.sequence
        || value
            .content()
            .content_hash()
            .map_err(|_| ClientError::InvalidResponse)?
            != value.content_hash()
        || filter.is_some_and(|filter| !matches_filter(*id, value, filter))
    {
        return Err(ClientError::InvalidResponse.into());
    }
    Ok(())
}
fn matches_filter(id: ClaimId, claim: &Claim, filter: &ListFilter) -> bool {
    filter.claim.is_none_or(|value| value == id)
        && filter
            .source
            .is_none_or(|value| Some(value) == claim.content().issuer())
        && filter
            .target
            .is_none_or(|value| Some(value) == claim.content().subject())
        && filter
            .status
            .is_none_or(|value| value == claim.lifecycle().status)
        && filter
            .action
            .is_none_or(|value| Some(value) == claim.content().action())
}
#[cfg(test)]
#[path = "claim_get_tests.rs"]
mod tests;
