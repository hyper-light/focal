//! One bounded disk owner per node, shared by every installed session policy.
//! Async calls require a Tokio runtime with IO and time drivers enabled.
use crate::custody::{
    Accounted, CustodyConfig, CustodyPolicy, CustodyScope, CustodyStore, content_error,
};
use crate::custody_prefix::{CustodyVerification, CustodyVerificationProgress, VerifiedCustody};
use focal_evidence::{ContentError, ContentStore, TransferManifest, UploadId};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use focal_model::*;
use focal_wire::*;
use futures_util::FutureExt;
use std::{
    future::Future,
    pin::Pin,
    sync::mpsc,
    thread::JoinHandle,
    time::{Duration, Instant},
};
use tokio::sync::oneshot;

#[derive(Clone)]
pub struct ContentHost {
    sender: mpsc::SyncSender<Work>,
    budget: MemoryBudget,
    timeout: Duration,
    max_frame_bytes: u32,
    chunk_bytes: usize,
    max_manifest_bytes: usize,
}
pub struct ContentOwner(JoinHandle<Result<(), AccessError>>);
impl ContentOwner {
    pub fn join(self) -> Result<(), AccessError> {
        self.0.join().map_err(|_| AccessError::Unavailable)?
    }
}
enum Command {
    Request(Box<VerifiedRequest>),
    Install(CustodyPolicy),
    Replace(Option<CustodyScope>, CustodyPolicy),
    Check(CustodyScope),
    Export(CustodyScope, ContentRef),
    Chunk(CustodyScope, ContentRef, usize),
    Read(CustodyScope, ContentRef, usize),
    Seal(CustodyScope, Box<VerifiedRequest>),
    /// Seal an inline legacy payload with the canonical import chunking.
    SealImport(focal_model::ContentDomainId, Vec<u8>, usize),
    Verify(Box<CustodyVerification>),
    VerifyNative(Box<NativeVerification>),
    Stop,
}
/// One native artifact to seal and verify under the exclusive content writer.
pub(crate) struct NativeVerification {
    pub scope: CustodyScope,
    pub request: RequestKey,
    pub descriptor: focal_model::lifecycle::artifact_descriptor::ArtifactDescriptor,
    pub domain: focal_model::ContentDomainId,
}
enum Output {
    Response(Box<Accounted<ResponseEnvelope>>),
    Manifest(Accounted<TransferManifest>),
    Bytes(Accounted<Vec<u8>>),
    LocalSeal(ContentRef),
    Done,
    Verification(CustodyVerificationProgress),
    NativeEvidence(Box<focal_evidence::VerifiedNativeArtifact>),
}
struct Work {
    command: Command,
    result: oneshot::Sender<Result<Output, AccessError>>,
    _allocation: Allocation,
}
impl ContentHost {
    /// Trusted, exact-prefix verification. The target comes from the committed
    /// session snapshot, while expected_active fences concurrent ingress-policy
    /// replacement. This operation does not install or strengthen active policy.
    pub async fn begin_verification(
        &self,
        snapshot: focal_ledger::DurableEvidenceSnapshot,
        expected_active: Option<CustodyScope>,
    ) -> Result<CustodyVerificationProgress, AccessError> {
        let verification = CustodyVerification::new(snapshot, expected_active, &self.budget)?;
        self.advance_verification(Box::new(verification)).await
    }
    pub async fn advance_verification(
        &self,
        verification: Box<CustodyVerification>,
    ) -> Result<CustodyVerificationProgress, AccessError> {
        match self.call(Command::Verify(verification), 0, true).await? {
            Output::Verification(progress) => Ok(progress),
            _ => Err(AccessError::Unavailable),
        }
    }
    pub async fn verify_prefix(
        &self,
        snapshot: focal_ledger::DurableEvidenceSnapshot,
        expected_active: Option<CustodyScope>,
    ) -> Result<VerifiedCustody, AccessError> {
        let mut progress = self.begin_verification(snapshot, expected_active).await?;
        loop {
            match progress {
                CustodyVerificationProgress::Complete(witness) => return Ok(*witness),
                CustodyVerificationProgress::Pending(verification) => {
                    progress = self.advance_verification(verification).await?;
                }
            }
        }
    }
    pub fn spawn(
        store: ContentStore,
        config: CustodyConfig,
        limits: WireLimits,
        budget: MemoryBudget,
    ) -> Result<(Self, ContentOwner), AccessError> {
        limits.validate()?;
        let queue_items = 32usize;
        let queue_charge = budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                queue_items
                    .checked_mul(size_of::<Work>())
                    .and_then(|n| n.checked_add(4096))
                    .ok_or(AccessError::Capacity)?,
            )
            .map_err(|_| AccessError::Capacity)?
            .commit();
        let (chunk_bytes, max_manifest_bytes) =
            (store.upload_chunk_bytes(), store.max_manifest_bytes());
        let owner = CustodyStore::new(store, config, budget.clone())?;
        let (sender, receiver) = mpsc::sync_channel::<Work>(queue_items);
        let timeout = limits.request_timeout;
        let max_frame_bytes = limits.max_frame_bytes;
        let worker_budget = budget.clone();
        let thread = std::thread::Builder::new()
            .name("focal-content".into())
            .spawn(move || {
                let _queue = queue_charge;
                let mut owner = owner;
                let mut next_expiry = Instant::now();
                loop {
                    let now = Instant::now();
                    if now >= next_expiry {
                        owner.expire(now)?;
                        next_expiry = now
                            .checked_add(Duration::from_millis(250))
                            .ok_or(AccessError::Unavailable)?;
                    }
                    match receiver
                        .recv_timeout(next_expiry.saturating_duration_since(Instant::now()))
                    {
                        Ok(work) => {
                            let Work {
                                command,
                                result,
                                _allocation: allocation,
                            } = work;
                            let stop = matches!(command, Command::Stop);
                            let outcome = execute(&mut owner, command, &limits, &worker_budget);
                            // Requests and decode staging are gone before the
                            // caller observes completion. Output carries its
                            // separate owned permit through the handoff.
                            drop(allocation);
                            let _ = result.send(outcome);
                            if stop {
                                return Ok(());
                            }
                        }
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                        Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
                    }
                }
            })
            .map_err(|_| AccessError::Unavailable)?;
        // Budget counters are shared with the actual disk worker so permits can
        // be dropped by either caller or worker after an async cancellation.
        Ok((
            Self {
                sender,
                budget,
                timeout,
                max_frame_bytes,
                chunk_bytes,
                max_manifest_bytes,
            },
            ContentOwner(thread),
        ))
    }
    async fn call(
        &self,
        command: Command,
        bytes: usize,
        control: bool,
    ) -> Result<Output, AccessError> {
        tokio::runtime::Handle::try_current().map_err(|_| AccessError::Unavailable)?;
        std::panic::catch_unwind(|| drop(tokio::time::sleep(Duration::ZERO)))
            .map_err(|_| AccessError::Unavailable)?;
        let allocation = self
            .budget
            .reserve(
                BudgetKind::Pending,
                if control {
                    BudgetLane::Completion
                } else {
                    BudgetLane::Ordinary
                },
                bytes.checked_add(4096).ok_or(AccessError::Capacity)?,
            )
            .map_err(|_| AccessError::Capacity)?
            .commit();
        let (send, receive) = oneshot::channel();
        self.sender
            .try_send(Work {
                command,
                result: send,
                _allocation: allocation,
            })
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => AccessError::Capacity,
                mpsc::TrySendError::Disconnected(_) => AccessError::Unavailable,
            })?;
        std::panic::AssertUnwindSafe(async { tokio::time::timeout(self.timeout, receive).await })
            .catch_unwind()
            .await
            .map_err(|_| AccessError::OutcomeUnknown)?
            .map_err(|_| AccessError::OutcomeUnknown)?
            .map_err(|_| AccessError::OutcomeUnknown)?
    }
    pub async fn install_policy(&self, policy: CustodyPolicy) -> Result<(), AccessError> {
        let charge = policy
            .peers
            .len()
            .checked_mul(128)
            .ok_or(AccessError::Capacity)?;
        match self.call(Command::Install(policy), charge, true).await? {
            Output::Done => Ok(()),
            _ => Err(AccessError::Unavailable),
        }
    }
    /// Trusted committed placement installation; no wire caller can invoke it.
    /// Exact-target retries reconcile a response lost after the content CAS.
    pub async fn replace_policy(
        &self,
        expected: Option<CustodyScope>,
        policy: CustodyPolicy,
    ) -> Result<(), AccessError> {
        let bytes = policy
            .peers
            .len()
            .checked_mul(128)
            .ok_or(AccessError::Capacity)?;
        match self
            .call(Command::Replace(expected, policy), bytes, true)
            .await?
        {
            Output::Done => Ok(()),
            _ => Err(AccessError::Unavailable),
        }
    }
    /// Seal and verify one native artifact's inline payload under the installed
    /// custody policy, returning the evidence the replicated owner requires.
    pub(crate) async fn verify_native(
        &self,
        verification: NativeVerification,
    ) -> Result<focal_evidence::VerifiedNativeArtifact, AccessError> {
        let bytes = verification
            .descriptor
            .retained_bytes()
            .map_err(|_| AccessError::Capacity)?
            .checked_mul(4)
            .and_then(|n| n.checked_add(self.chunk_bytes.saturating_mul(4)))
            .ok_or(AccessError::Capacity)?;
        match self
            .call(Command::VerifyNative(Box::new(verification)), bytes, false)
            .await?
        {
            Output::NativeEvidence(evidence) => Ok(*evidence),
            _ => Err(AccessError::Unavailable),
        }
    }
    pub(crate) async fn check_policy(&self, scope: CustodyScope) -> Result<(), AccessError> {
        match self.call(Command::Check(scope), 0, true).await? {
            Output::Done => Ok(()),
            _ => Err(AccessError::Unavailable),
        }
    }
    pub async fn request(
        &self,
        request: VerifiedRequest,
    ) -> Result<Accounted<ResponseEnvelope>, AccessError> {
        let bytes = postcard::experimental::serialized_size(request.request())
            .map_err(|_| AccessError::InvalidRequest)?;
        if bytes > self.max_frame_bytes as usize {
            return Err(AccessError::Capacity);
        }
        let control = matches!(
            request.request().operation,
            Operation::Custody(
                CustodyRequest::Seal { .. }
                    | CustodyRequest::Verify { .. }
                    | CustodyRequest::Cancel { .. }
            )
        );
        match self
            .call(
                Command::Request(Box::new(request)),
                bytes.checked_mul(4).ok_or(AccessError::Capacity)?,
                control,
            )
            .await?
        {
            Output::Response(response) => Ok(*response),
            _ => Err(AccessError::Unavailable),
        }
    }
    pub async fn export_manifest(
        &self,
        scope: CustodyScope,
        content: ContentRef,
    ) -> Result<Accounted<TransferManifest>, AccessError> {
        match self.call(Command::Export(scope, content), 0, false).await? {
            Output::Manifest(value) => Ok(value),
            _ => Err(AccessError::Unavailable),
        }
    }
    /// Export the manifest first. An expired/invalidated cache returns unavailable
    /// and requires an explicit re-export; no hidden per-chunk manifest parsing.
    pub async fn read_transfer_chunk(
        &self,
        scope: CustodyScope,
        content: ContentRef,
        index: usize,
    ) -> Result<Accounted<Vec<u8>>, AccessError> {
        match self
            .call(Command::Chunk(scope, content, index), 0, false)
            .await?
        {
            Output::Bytes(value) => Ok(value),
            _ => Err(AccessError::Unavailable),
        }
    }
    pub async fn read_bytes(
        &self,
        scope: CustodyScope,
        content: ContentRef,
        max_bytes: usize,
    ) -> Result<Accounted<Vec<u8>>, AccessError> {
        match self
            .call(Command::Read(scope, content, max_bytes), 0, false)
            .await?
        {
            Output::Bytes(value) => Ok(value),
            _ => Err(AccessError::Unavailable),
        }
    }
    /// Only the coordinator can use this local durability seam. It must still
    /// establish required remote custody before returning UploadReply::Sealed.
    pub(crate) async fn seal_upload(
        &self,
        scope: CustodyScope,
        request: VerifiedRequest,
    ) -> Result<ContentRef, AccessError> {
        if !matches!(
            request.request().operation,
            Operation::Upload(UploadRequest::Seal { .. })
        ) {
            return Err(AccessError::InvalidRequest);
        }
        match self
            .call(Command::Seal(scope, Box::new(request)), 0, true)
            .await?
        {
            Output::LocalSeal(value) => Ok(value),
            _ => Err(AccessError::Unavailable),
        }
    }
    /// The store's chunk size and manifest bound: the parameters an import
    /// proposal records so every replica seals legacy payloads identically.
    pub fn import_chunking(&self) -> (usize, usize) {
        (self.chunk_bytes, self.max_manifest_bytes)
    }
    /// Seal one inline legacy payload before an import is proposed or applied
    /// (23 §5.2). Idempotent: an already sealed payload installs nothing new.
    pub async fn seal_import_inline(
        &self,
        domain: focal_model::ContentDomainId,
        bytes: Vec<u8>,
        chunk_bytes: usize,
    ) -> Result<focal_model::ContentRef, AccessError> {
        let size = bytes.len();
        match self
            .call(Command::SealImport(domain, bytes, chunk_bytes), size, true)
            .await?
        {
            Output::LocalSeal(value) => Ok(value),
            _ => Err(AccessError::Unavailable),
        }
    }
    pub async fn stop(&self) -> Result<(), AccessError> {
        match self.call(Command::Stop, 0, true).await? {
            Output::Done => Ok(()),
            _ => Err(AccessError::Unavailable),
        }
    }
}
impl RequestHandler for ContentHost {
    fn handle(
        &self,
        request: VerifiedRequest,
    ) -> Pin<Box<dyn Future<Output = ResponseEnvelope> + Send + '_>> {
        Box::pin(async move { self.handle_accounted(request).await.into_envelope() })
    }
    fn handle_accounted(&self, request: VerifiedRequest) -> OwnedHandlerFuture<'_> {
        Box::pin(async move {
            let fallback = request
                .request()
                .reply(Response::Error(AccessError::Unavailable));
            match self.request(request).await {
                Ok(response) => response.into_wire_response(),
                Err(error) => OwnedResponse::new(ResponseEnvelope {
                    result: Response::Error(error),
                    ..fallback
                }),
            }
        })
    }
}
fn native_evidence_error(error: focal_evidence::NativeEvidenceError) -> AccessError {
    use focal_evidence::NativeEvidenceError;
    match error {
        NativeEvidenceError::Content(error) => crate::custody::content_error(error),
        NativeEvidenceError::Memory(_) => AccessError::Capacity,
        NativeEvidenceError::Contract(_)
        | NativeEvidenceError::Schema(_)
        | NativeEvidenceError::WrongRequest => AccessError::InvalidRequest,
        NativeEvidenceError::VerificationBudgetChanged => AccessError::Unavailable,
    }
}
fn execute(
    owner: &mut CustodyStore,
    command: Command,
    limits: &WireLimits,
    budget: &MemoryBudget,
) -> Result<Output, AccessError> {
    match command {
        Command::Install(policy) => {
            owner.install_policy(policy)?;
            Ok(Output::Done)
        }
        Command::Replace(expected, policy) => {
            owner.replace_policy(expected, policy)?;
            Ok(Output::Done)
        }
        Command::Check(scope) => {
            owner.check_policy(scope)?;
            Ok(Output::Done)
        }
        Command::VerifyNative(verification) => {
            owner.check_policy(verification.scope)?;
            if verification.domain
                != focal_model::ContentDomainId(verification.scope.ledger.tenant.0)
            {
                return Err(AccessError::Unauthorized);
            }
            let evidence = owner
                .content_mut()
                .verify_native_artifact(
                    verification.request,
                    &verification.descriptor,
                    verification.domain,
                    budget,
                    &focal_evidence::BuiltinNativeSchemas,
                )
                .map_err(native_evidence_error)?;
            Ok(Output::NativeEvidence(Box::new(evidence)))
        }
        Command::Export(scope, content) => {
            owner.export_manifest(scope, content).map(Output::Manifest)
        }
        Command::Chunk(scope, content, index) => owner
            .read_transfer_chunk(scope, content, index)
            .map(Output::Bytes),
        Command::Read(scope, content, limit) => {
            owner.read_bytes(scope, content, limit).map(Output::Bytes)
        }
        Command::Seal(scope, request) => {
            if owner.authorize(&request)? != scope {
                return Err(AccessError::Unavailable);
            }
            let Operation::Upload(UploadRequest::Seal { upload }) = &request.request().operation
            else {
                return Err(AccessError::InvalidRequest);
            };
            let id = UploadId(upload_scope(
                request.peer(),
                request.request().ledger,
                *upload,
            ));
            let amount = owner
                .content()
                .max_manifest_bytes()
                .checked_mul(4)
                .and_then(|n| {
                    owner
                        .content()
                        .max_chunk_bytes()
                        .checked_mul(2)
                        .and_then(|c| n.checked_add(c))
                })
                .and_then(|n| n.checked_add(4096))
                .ok_or(AccessError::Capacity)?;
            let _scan = budget
                .reserve(BudgetKind::Payload, BudgetLane::Completion, amount)
                .map_err(|_| AccessError::Capacity)?;
            owner
                .content_mut()
                .seal(id)
                .map(Output::LocalSeal)
                .map_err(content_error)
        }
        Command::SealImport(domain, bytes, chunk_bytes) => {
            let amount = bytes
                .len()
                .checked_mul(2)
                .and_then(|n| n.checked_add(owner.content().max_manifest_bytes()))
                .and_then(|n| n.checked_add(4096))
                .ok_or(AccessError::Capacity)?;
            let _scan = budget
                .reserve(BudgetKind::Payload, BudgetLane::Completion, amount)
                .map_err(|_| AccessError::Capacity)?;
            owner
                .content_mut()
                .seal_import_inline(domain, &bytes, chunk_bytes)
                .map(Output::LocalSeal)
                .map_err(content_error)
        }
        Command::Stop => Ok(Output::Done),
        Command::Verify(verification) => verification
            .advance(owner, budget)
            .map(Output::Verification),
        Command::Request(request) => handle_request(owner, &request, limits, budget)
            .map(|reply| Output::Response(Box::new(reply))),
    }
}
fn handle_request(
    owner: &mut CustodyStore,
    verified: &VerifiedRequest,
    limits: &WireLimits,
    budget: &MemoryBudget,
) -> Result<Accounted<ResponseEnvelope>, AccessError> {
    let scope = owner.authorize(verified)?;
    let request = verified.request();
    if matches!(request.operation, Operation::Custody(_)) {
        return owner
            .request(verified)
            .map(|value| value.map(|reply| request.reply(Response::Custody(reply))));
    }
    if matches!(
        request.operation,
        Operation::Upload(UploadRequest::Seal { .. })
    ) {
        return Err(AccessError::UnsupportedOperation);
    }
    let output = match &request.operation {
        Operation::Download { max_bytes, .. } => {
            ((*max_bytes).min(limits.max_frame_bytes.saturating_sub(256)) as usize)
                .min(owner.content().upload_chunk_bytes())
        }
        _ => 0,
    };
    let amount = output
        .checked_mul(3)
        .ok_or(AccessError::Capacity)?
        .checked_add(owner.content().max_chunk_bytes())
        .and_then(|n| {
            owner
                .content()
                .max_manifest_bytes()
                .checked_mul(4)
                .and_then(|m| n.checked_add(m))
        })
        .and_then(|n| n.checked_add(4096))
        .ok_or(AccessError::Capacity)?;
    let allocation = budget
        .reserve(BudgetKind::Query, BudgetLane::Ordinary, amount)
        .map_err(|_| AccessError::Capacity)?
        .commit();
    let response = match &request.operation {
        Operation::Download {
            content,
            offset,
            max_bytes,
        } => {
            owner.check_scope(scope, content)?;
            let cap = ((*max_bytes).min(limits.max_frame_bytes.saturating_sub(256)) as usize)
                .min(owner.content().upload_chunk_bytes());
            let bytes = owner
                .content()
                .read_range(content, *offset, cap)
                .map_err(content_error)?;
            let through = offset
                .checked_add(bytes.len() as u64)
                .ok_or(AccessError::Capacity)?;
            Response::Content(ContentChunk {
                offset: *offset,
                eof: through == content.length,
                bytes,
            })
        }
        Operation::Upload(upload) => {
            let id = UploadId(upload_scope(
                verified.peer(),
                request.ledger,
                upload.upload(),
            ));
            let reply = match upload {
                UploadRequest::Begin {
                    length,
                    digest,
                    class,
                    ..
                } => owner
                    .content_mut()
                    .begin(
                        id,
                        ContentDomainId(request.ledger.tenant.0),
                        *class,
                        *length,
                        Some(*digest),
                    )
                    .map(UploadReply::Offset)
                    .map_err(content_error)?,
                UploadRequest::Append { offset, bytes, .. } => match owner
                    .content_mut()
                    .append(id, *offset, bytes)
                {
                    Ok(offset) | Err(ContentError::Offset(offset)) => UploadReply::Offset(offset),
                    Err(error) => return Err(content_error(error)),
                },
                UploadRequest::Cancel { .. } => {
                    owner.content_mut().finish(id).map_err(content_error)?;
                    UploadReply::Cancelled
                }
                UploadRequest::Seal { .. } => return Err(AccessError::UnsupportedOperation),
            };
            Response::Upload(reply)
        }
        _ => return Err(AccessError::UnsupportedOperation),
    };
    let reply = request.reply(response);
    if postcard::experimental::serialized_size(&reply).map_err(|_| AccessError::InvalidRequest)?
        > limits.max_frame_bytes as usize
    {
        return Err(AccessError::Capacity);
    }
    Ok(owner.accounted(reply, allocation))
}

#[cfg(test)]
#[path = "custody_tests.rs"]
mod tests;
