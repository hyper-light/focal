use crate::*;
use focal_model::*;
use focal_wire::{WireError, encode_payload, validate_response};
use std::{collections::BTreeMap, sync::Mutex, time::Duration};

#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub max_elapsed: Duration,
    pub base_backoff: Duration,
    pub max_backoff: Duration,
}
impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 4,
            max_elapsed: Duration::from_secs(30),
            base_backoff: Duration::from_millis(20),
            max_backoff: Duration::from_millis(500),
        }
    }
}

/// A committed managed receipt or a transient domain response. A Domain value
/// never releases an issued ordinal and is not a historical receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManagedSubmitOutcome {
    Committed(Box<ManagedReply>),
    Domain(DomainOutcome),
}

pub enum ClientError {
    /// The exact request is retained for application-controlled retry, even when
    /// the caller can no longer know whether an earlier attempt committed.
    OutcomeUnknown {
        request: Box<RequestEnvelope>,
    },
    Access(AccessError),
    Unauthenticated,
    InvalidResponse,
    Transport,
    Configuration,
}
impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OutcomeUnknown { request } => write!(
                f,
                "outcome unknown for request {}; retry the same identity",
                request.request_id
            ),
            Self::Access(error) => write!(f, "{error}"),
            Self::Unauthenticated => write!(
                f,
                "peer or client authentication failed; check the selected context's credentials and trust"
            ),
            Self::InvalidResponse => write!(f, "invalid server response"),
            Self::Transport => write!(f, "transport unavailable"),
            Self::Configuration => write!(f, "invalid client limits"),
        }
    }
}
impl std::fmt::Debug for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::error::Error for ClientError {}

struct CachedRoute {
    hint: RouteHint,
    used: u64,
}
struct Routes {
    entries: BTreeMap<LedgerId, CachedRoute>,
    clock: u64,
    capacity: usize,
}
impl Routes {
    fn get(&mut self, ledger: LedgerId) -> Option<RouteHint> {
        self.clock = self.clock.saturating_add(1);
        let entry = self.entries.get_mut(&ledger)?;
        entry.used = self.clock;
        Some(entry.hint.clone())
    }
    fn insert(&mut self, ledger: LedgerId, hint: RouteHint) -> Result<(), ClientError> {
        if let Some(old) = self.entries.get(&ledger)
            && (hint.epoch < old.hint.epoch || (hint.epoch == old.hint.epoch && hint != old.hint))
        {
            return Err(ClientError::InvalidResponse);
        }
        self.clock = self.clock.saturating_add(1);
        if !self.entries.contains_key(&ledger)
            && self.entries.len() >= self.capacity
            && let Some(key) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.used)
                .map(|(key, _)| *key)
        {
            self.entries.remove(&key);
        }
        self.entries.insert(
            ledger,
            CachedRoute {
                hint,
                used: self.clock,
            },
        );
        Ok(())
    }
}

pub struct Client<T: ClientTransport> {
    transport: T,
    policy: RetryPolicy,
    limits: WireLimits,
    routes: Mutex<Routes>,
}
impl<T: ClientTransport> Client<T> {
    pub(crate) fn wire_limits(&self) -> &WireLimits {
        &self.limits
    }
    pub(crate) fn retry_timeout(&self) -> Duration {
        self.policy.max_elapsed
    }
    /// Read bounded scalar counts after a fresh quorum barrier in this ledger.
    /// The returned token identifies the observation without retaining a snapshot.
    pub async fn ledger_summary(
        &self,
        request: RequestEnvelope,
    ) -> Result<focal_wire::LedgerSummary, ClientError> {
        if !matches!(request.operation, Operation::Summary) {
            return Err(ClientError::Configuration);
        }
        match self.request(request).await?.result {
            Response::Summary(value) => Ok(value),
            _ => Err(ClientError::InvalidResponse),
        }
    }
    pub fn new(
        transport: T,
        policy: RetryPolicy,
        limits: WireLimits,
        route_capacity: usize,
    ) -> Result<Self, ClientError> {
        if policy.max_attempts == 0
            || policy.max_attempts > 32
            || policy.max_elapsed.is_zero()
            || policy.max_elapsed > Duration::from_secs(3600)
            || policy.max_backoff > policy.max_elapsed
            || policy.base_backoff > policy.max_backoff
            || route_capacity == 0
            || route_capacity > 65536
            || limits.validate().is_err()
        {
            return Err(ClientError::Configuration);
        }
        Ok(Self {
            transport,
            policy,
            limits,
            routes: Mutex::new(Routes {
                entries: BTreeMap::new(),
                clock: 0,
                capacity: route_capacity,
            }),
        })
    }
    pub async fn submit(&self, request: RequestEnvelope) -> Result<MutationReply, ClientError> {
        if !matches!(
            request.operation,
            Operation::Submit { .. } | Operation::OpenEpoch { .. }
        ) {
            return Err(ClientError::Configuration);
        }
        match self.request(request).await?.result {
            Response::Submitted(reply) => Ok(reply),
            _ => Err(ClientError::InvalidResponse),
        }
    }
    pub async fn read(&self, request: RequestEnvelope) -> Result<ReadPage, ClientError> {
        if !matches!(request.operation, Operation::Read(_)) {
            return Err(ClientError::Configuration);
        }
        match self.request(request).await?.result {
            Response::Read(page) => Ok(page),
            _ => Err(ClientError::InvalidResponse),
        }
    }
    /// Inspect immutable requirement bindings of externally invoked handlers.
    pub async fn validators(
        &self,
        request: RequestEnvelope,
    ) -> Result<focal_wire::ListPage, ClientError> {
        if !matches!(request.operation, Operation::Validators(_)) {
            return Err(ClientError::Configuration);
        }
        match self.request(request).await?.result {
            Response::Validators(page) => Ok(page),
            _ => Err(ClientError::InvalidResponse),
        }
    }
    /// Submit an already journaled managed request. The journal owns filesystem
    /// work; this method only waits for the authenticated wire result.
    pub async fn submit_managed(
        &self,
        request: RequestEnvelope,
        context: crate::pending::OperationContext,
    ) -> Result<ManagedReply, ClientError> {
        match self.submit_managed_outcome(request, context).await? {
            ManagedSubmitOutcome::Committed(reply) => Ok(*reply),
            ManagedSubmitOutcome::Domain(_) => Err(ClientError::InvalidResponse),
        }
    }
    /// Preserve Refuse/Inform without treating them as committed outcomes. The
    /// caller keeps the original journal pending until a receipt or exact seal.
    pub async fn submit_managed_outcome(
        &self,
        request: RequestEnvelope,
        context: crate::pending::OperationContext,
    ) -> Result<ManagedSubmitOutcome, ClientError> {
        let (key, _, _) = focal_wire::managed_request_identity(&request)
            .map_err(|_| ClientError::Configuration)?;
        if key.stream.cluster != context.cluster
            || key.stream.principal != context.principal
            || key.stream.ledger != context.ledger
        {
            return Err(ClientError::Configuration);
        }
        match self.request(request).await?.result {
            Response::Managed(reply) => Ok(ManagedSubmitOutcome::Committed(Box::new(reply))),
            Response::Submitted(MutationReply::Domain(
                outcome @ (DomainOutcome::Refuse { .. } | DomainOutcome::Inform { .. }),
            )) => Ok(ManagedSubmitOutcome::Domain(outcome)),
            _ => Err(ClientError::InvalidResponse),
        }
    }
    pub async fn request_stream_control(
        &self,
        request: RequestEnvelope,
        context: crate::pending::OperationContext,
    ) -> Result<RequestStreamControlReply, ClientError> {
        if !matches!(request.operation, Operation::RequestStreamControl { cluster, .. } if cluster == context.cluster)
            || request.ledger != context.ledger
            || context.principal.is_zero()
            || context.cluster == [0; 16]
        {
            return Err(ClientError::Configuration);
        }
        match self.request(request).await?.result {
            Response::RequestStreamControlled(reply)
                if reply.receipt.principal == context.principal =>
            {
                Ok(reply)
            }
            _ => Err(ClientError::InvalidResponse),
        }
    }
    pub async fn request_stream_read(
        &self,
        request: RequestEnvelope,
        context: crate::pending::OperationContext,
    ) -> Result<RequestStreamReadReply, ClientError> {
        if !matches!(request.operation, Operation::RequestStreamRead { cluster, .. } if cluster == context.cluster)
            || request.ledger != context.ledger
            || context.principal.is_zero()
            || context.cluster == [0; 16]
        {
            return Err(ClientError::Configuration);
        }
        match self.request(request).await?.result {
            Response::RequestStreamRead(reply) if reply.page.principal == context.principal => {
                Ok(reply)
            }
            _ => Err(ClientError::InvalidResponse),
        }
    }
    /// Reconcile only the adapter's authenticated principal. A transport can
    /// validate reply shape without knowing that identity; this entry point
    /// additionally binds it before exposing any historical receipt.
    pub async fn reconcile(
        &self,
        request: RequestEnvelope,
        expected_principal: ParticipantId,
    ) -> Result<ReconcileReply, ClientError> {
        if !matches!(request.operation, Operation::Reconcile(_)) || expected_principal.is_zero() {
            return Err(ClientError::Configuration);
        }
        // request() binds every other field against the actual routed envelope,
        // whose route epoch may advance through authenticated route discovery.
        let response = self.request(request).await?;
        match response.result {
            Response::Reconciled(reply) if reply.page.principal == expected_principal => Ok(reply),
            _ => Err(ClientError::InvalidResponse),
        }
    }
    pub async fn subscribe(
        &self,
        request: RequestEnvelope,
    ) -> Result<SubscriptionBatch, ClientError> {
        if !matches!(request.operation, Operation::Subscribe(_)) {
            return Err(ClientError::Configuration);
        }
        match self.request(request).await?.result {
            Response::Subscription(batch) => Ok(batch),
            _ => Err(ClientError::InvalidResponse),
        }
    }
    pub async fn stream(&self, request: RequestEnvelope) -> Result<StreamReply, ClientError> {
        if !matches!(request.operation, Operation::Stream(_)) {
            return Err(ClientError::Configuration);
        }
        match self.request(request).await?.result {
            Response::Stream(reply) => Ok(reply),
            _ => Err(ClientError::InvalidResponse),
        }
    }
    pub async fn traverse(
        &self,
        request: RequestEnvelope,
    ) -> Result<focal_wire::TraversalPage, ClientError> {
        if !matches!(request.operation, Operation::Traverse(_)) {
            return Err(ClientError::Configuration);
        }
        match self.request(request).await?.result {
            Response::Traversed(page) => Ok(page),
            _ => Err(ClientError::InvalidResponse),
        }
    }
    pub async fn upload(&self, request: RequestEnvelope) -> Result<UploadReply, ClientError> {
        if !matches!(request.operation, Operation::Upload(_)) {
            return Err(ClientError::Configuration);
        }
        match self.request(request).await?.result {
            Response::Upload(reply) => Ok(reply),
            _ => Err(ClientError::InvalidResponse),
        }
    }
    pub async fn download(&self, request: RequestEnvelope) -> Result<ContentChunk, ClientError> {
        if !matches!(request.operation, Operation::Download { .. }) {
            return Err(ClientError::Configuration);
        }
        match self.request(request).await?.result {
            Response::Content(chunk) => Ok(chunk),
            _ => Err(ClientError::InvalidResponse),
        }
    }
    /// Retries never regenerate request IDs, epochs, or command/evidence bytes.
    /// A cancellation drops this wait; it does not retract a submitted mutation.
    pub async fn request(
        &self,
        mut request: RequestEnvelope,
    ) -> Result<ResponseEnvelope, ClientError> {
        use std::{
            future::{Future, poll_fn},
            panic::{AssertUnwindSafe, catch_unwind},
            task::Poll,
        };
        let mutation = request.operation.is_mutation();
        let mut write_uncertain = false;
        // A missing Tokio driver or a transport dependency failure must not
        // unwind through an SDK caller. After entering a mutation exchange,
        // failure cannot establish that no bytes reached the authority.
        let result = {
            let mut exchange =
                std::pin::pin!(self.request_inner(&mut request, &mut write_uncertain));
            poll_fn(
                |cx| match catch_unwind(AssertUnwindSafe(|| exchange.as_mut().poll(cx))) {
                    Ok(poll) => poll.map(Ok),
                    Err(_) => Poll::Ready(Err(())),
                },
            )
            .await
        };
        match result {
            Ok(Err(_)) if write_uncertain => Err(ClientError::OutcomeUnknown {
                request: Box::new(request),
            }),
            Ok(result) => result,
            Err(()) if mutation => Err(ClientError::OutcomeUnknown {
                request: Box::new(request),
            }),
            Err(()) => Err(ClientError::Transport),
        }
    }
    async fn request_inner(
        &self,
        request: &mut RequestEnvelope,
        write_uncertain: &mut bool,
    ) -> Result<ResponseEnvelope, ClientError> {
        if tokio::runtime::Handle::try_current().is_err() {
            return Err(ClientError::Transport);
        }
        encode_payload(&request, self.limits.max_frame_bytes)
            .map_err(|_| ClientError::Access(AccessError::Capacity))?;
        let mut route = self
            .routes
            .lock()
            .map_err(|_| ClientError::Transport)?
            .get(request.ledger);
        if let Some(hint) = &route {
            if hint.epoch >= request.route_epoch {
                request.route_epoch = hint.epoch;
            } else {
                route = None;
            }
        }
        let start = tokio::time::Instant::now();
        let mut uncertain = false;
        for attempt in 0..self.policy.max_attempts {
            let remaining = self.policy.max_elapsed.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                break;
            }
            let response =
                tokio::time::timeout(remaining, self.transport.request(route.as_ref(), request))
                    .await;
            match response {
                Ok(Ok(response)) => {
                    if validate_response(request, &response, None, &self.limits).is_err() {
                        if request.operation.is_mutation() {
                            break;
                        }
                        return Err(ClientError::InvalidResponse);
                    }
                    match &response.result {
                        Response::Error(AccessError::RouteChanged(hint)) => {
                            if hint.epoch < request.route_epoch {
                                return Err(ClientError::InvalidResponse);
                            }
                            if route.as_ref() == Some(hint) {
                                return Err(ClientError::Access(AccessError::RouteChanged(
                                    hint.clone(),
                                )));
                            }
                            self.routes
                                .lock()
                                .map_err(|_| ClientError::Transport)?
                                .insert(request.ledger, hint.clone())?;
                            request.route_epoch = hint.epoch;
                            route = Some(hint.clone());
                        }
                        Response::Error(AccessError::OutcomeUnknown)
                        | Response::Submitted(MutationReply::Pending(_)) => {
                            uncertain = true;
                            *write_uncertain = request.operation.is_mutation();
                        }
                        Response::Submitted(MutationReply::Domain(
                            DomainOutcome::Refuse { .. } | DomainOutcome::Inform { .. },
                        )) if uncertain && request.operation.is_mutation() => break,
                        Response::Error(AccessError::Unavailable) => {}
                        Response::Error(error) => {
                            if uncertain && request.operation.is_mutation() {
                                break;
                            }
                            return Err(ClientError::Access(error.clone()));
                        }
                        _ => return Ok(response),
                    }
                }
                Ok(Err(WireError::Access(error))) => {
                    if uncertain && request.operation.is_mutation() {
                        break;
                    }
                    return Err(ClientError::Access(error));
                }
                Ok(Err(WireError::Limit | WireError::InvalidFrame)) => {
                    if request.operation.is_mutation() {
                        break;
                    }
                    return Err(ClientError::InvalidResponse);
                }
                Ok(Err(WireError::Authentication)) => {
                    if uncertain && request.operation.is_mutation() {
                        break;
                    }
                    return Err(ClientError::Unauthenticated);
                }
                Ok(Err(WireError::Io(_) | WireError::Connection | WireError::Timeout)) | Err(_) => {
                    uncertain = true;
                    *write_uncertain = request.operation.is_mutation();
                }
            }
            if attempt.saturating_add(1) < self.policy.max_attempts {
                let backoff = self
                    .policy
                    .base_backoff
                    .saturating_mul(1u32 << attempt.min(16))
                    .min(self.policy.max_backoff)
                    .min(self.policy.max_elapsed.saturating_sub(start.elapsed()));
                tokio::time::sleep(backoff).await;
            }
        }
        if request.operation.is_mutation() {
            *write_uncertain = true;
            Err(ClientError::Transport)
        } else {
            Err(ClientError::Transport)
        }
    }
}
