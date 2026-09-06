//! A bounded read-only observer. It neither creates monitors nor executes timers.
use crate::{Client, ClientError, ClientTransport, claim_get::ClaimGetError};
use focal_model::{ClaimId, ClaimStatus, ObjectKind, ObjectRevision};
use focal_wire::*;
use serde::{Deserialize, Serialize};
use std::{
    future::{Future, poll_fn},
    panic::{AssertUnwindSafe, catch_unwind},
    pin::pin,
    task::Poll,
    time::Duration,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimWaitUntil {
    Satisfied,
    Terminal,
    Released,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClaimWaitCondition {
    Met,
    Pending,
    Unmet,
}
impl ClaimWaitCondition {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Met => "Met",
            Self::Pending => "Pending",
            Self::Unmet => "Unmet",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimObservation {
    pub token: ReadToken,
    pub id: ClaimId,
    pub status: ClaimStatus,
    pub revision: ObjectRevision,
    pub local_complete: bool,
    pub released: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimWaitResult {
    pub condition: ClaimWaitCondition,
    pub until: ClaimWaitUntil,
    pub observation: ClaimObservation,
    pub probes: u32,
}

#[derive(Debug, thiserror::Error)]
pub enum ClaimWaitError {
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error("claim was not found at the observed prefix")]
    NotFound,
    #[error("claim wait requires one fresh claim read and timeout_ms in 1..=30000")]
    InvalidRequest,
}
impl From<ClaimGetError> for ClaimWaitError {
    fn from(error: ClaimGetError) -> Self {
        match error {
            ClaimGetError::Client(error) => Self::Client(error),
            ClaimGetError::NotFound => Self::NotFound,
            ClaimGetError::InvalidRequest => Self::InvalidRequest,
            ClaimGetError::Incomplete => Self::Client(ClientError::Transport),
            ClaimGetError::Ambiguous => Self::Client(ClientError::InvalidResponse),
        }
    }
}

impl<T: ClientTransport> Client<T> {
    /// Observe at most 31 fresh quorum reads, with at least one second between
    /// completed probes. The whole wait uses min(timeout, retry deadline, 30s).
    /// Only the latest fixed-size observation survives between probes. Pending
    /// means this observer's deadline elapsed; it is not a business outcome.
    /// A terminal non-satisfied claim returns Unmet for Satisfied immediately.
    /// Dropping this future cancels only observation, with no journal or write.
    pub async fn claim_wait(
        &self,
        request: RequestEnvelope,
        until: ClaimWaitUntil,
        timeout: Duration,
    ) -> Result<ClaimWaitResult, ClaimWaitError> {
        let Operation::Read(ReadRequest {
            consistency: ReadConsistency::Linearizable,
            query: ReadQuery::Objects(references),
            max_items: 1,
        }) = &request.operation
        else {
            return Err(ClaimWaitError::InvalidRequest);
        };
        if request.ledger.tenant.is_zero()
            || request.ledger.session.is_zero()
            || request.request_id.is_zero()
            || request.request_epoch.0 == 0
            || request.route_epoch.0 == 0
            || !matches!(
                request.protocol,
                PROTOCOL_VERSION | MANAGED_PROTOCOL_VERSION | PEER_PROTOCOL_VERSION
            )
            || timeout < Duration::from_millis(1)
            || timeout > Duration::from_secs(30)
            || references.len() != 1
            || references.first().is_none_or(|reference| {
                reference.kind != ObjectKind::Claim
                    || reference.ledger != request.ledger
                    || reference.id.is_zero()
            })
        {
            return Err(ClaimWaitError::InvalidRequest);
        }
        // Contain a missing Tokio time driver without leaking an in-flight read.
        let future = self.wait_inner(request, until, timeout.min(self.retry_timeout()));
        let mut future = pin!(future);
        poll_fn(
            |cx| match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
                Ok(result) => result,
                Err(_) => Poll::Ready(Err(ClientError::Transport.into())),
            },
        )
        .await
    }

    async fn wait_inner(
        &self,
        mut request: RequestEnvelope,
        until: ClaimWaitUntil,
        timeout: Duration,
    ) -> Result<ClaimWaitResult, ClaimWaitError> {
        let deadline = tokio::time::Instant::now()
            .checked_add(timeout)
            .ok_or(ClaimWaitError::InvalidRequest)?;
        let seed = request.request_id;
        let mut latest: Option<ClaimWaitResult> = None;
        for probe in 0u32..31 {
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            if probe > 0 {
                let mut hash = blake3::Hasher::new_derive_key("focal.client.claim-wait.read.v1");
                hash.update(&seed.0);
                hash.update(&request.ledger.tenant.0);
                hash.update(&request.ledger.session.0);
                hash.update(&probe.to_be_bytes());
                let mut id = [0; 16];
                id.copy_from_slice(
                    hash.finalize()
                        .as_bytes()
                        .get(..16)
                        .ok_or(ClaimWaitError::InvalidRequest)?,
                );
                request.request_id = focal_model::RequestId(id);
                if request.request_id.is_zero() {
                    return Err(ClaimWaitError::InvalidRequest);
                }
            }
            let page =
                match tokio::time::timeout_at(deadline, self.claim_get(request.clone())).await {
                    Ok(result) => result?,
                    Err(_) => break,
                };
            let Some(ReadObject::Claim { id, value }) = page.objects.into_iter().next() else {
                return Err(ClientError::InvalidResponse.into());
            };
            let state = value.lifecycle();
            let observation = ClaimObservation {
                token: page.token,
                id,
                status: state.status,
                revision: state.revision,
                local_complete: state.local_complete,
                released: state.released,
            };
            if latest.is_some_and(|previous| {
                observation.id != previous.observation.id
                    || observation.token.sequence < previous.observation.token.sequence
                    || observation.token.route_epoch < previous.observation.token.route_epoch
                    || observation.revision < previous.observation.revision
                    || (previous.observation.status.is_terminal()
                        && observation.status != previous.observation.status)
                    || (previous.observation.released && !observation.released)
                    || (observation.token.sequence == previous.observation.token.sequence
                        && (observation.status != previous.observation.status
                            || observation.revision != previous.observation.revision
                            || observation.local_complete != previous.observation.local_complete
                            || observation.released != previous.observation.released))
            }) {
                return Err(ClientError::InvalidResponse.into());
            }
            let met = match until {
                ClaimWaitUntil::Satisfied => state.status == ClaimStatus::Satisfied,
                ClaimWaitUntil::Terminal => state.status.is_terminal(),
                ClaimWaitUntil::Released => state.released,
            };
            let condition = if met {
                ClaimWaitCondition::Met
            } else if until == ClaimWaitUntil::Satisfied && state.status.is_terminal() {
                ClaimWaitCondition::Unmet
            } else {
                ClaimWaitCondition::Pending
            };
            let result = ClaimWaitResult {
                condition,
                until,
                observation,
                probes: probe.checked_add(1).ok_or(ClaimWaitError::InvalidRequest)?,
            };
            if condition != ClaimWaitCondition::Pending {
                return Ok(result);
            }
            latest = Some(result);
            // The response and full immutable claim are released before sleeping.
            drop(value);
            let next = tokio::time::Instant::now()
                .checked_add(Duration::from_secs(1))
                .ok_or(ClaimWaitError::InvalidRequest)?;
            tokio::time::sleep_until(next.min(deadline)).await;
        }
        latest.ok_or_else(|| ClientError::Transport.into())
    }
}

#[cfg(test)]
#[path = "claim_wait_tests.rs"]
mod tests;
