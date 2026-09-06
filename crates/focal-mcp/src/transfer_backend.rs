use super::*;
use focal_client::artifact_transfer::{
    TRANSFER_CHUNK_BYTES, TransferError, UploadJournal, UploadSpec,
};
use focal_client::input::parse_hash;
use serde::Deserialize;
use std::{
    panic::AssertUnwindSafe,
    time::{Duration, Instant},
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Begin {
    upload_id: String,
    length: u64,
    digest: String,
    #[serde(default = "evidence")]
    class: String,
}
fn evidence() -> String {
    "evidence".into()
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Append {
    upload_id: String,
    offset: u64,
    bytes_hex: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Identity {
    upload_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Download {
    id: String,
    #[serde(default)]
    token: Option<ReadToken>,
    #[serde(default)]
    offset: u64,
    #[serde(default = "chunk_limit")]
    max_bytes: u32,
}
fn chunk_limit() -> u32 {
    TRANSFER_CHUNK_BYTES as u32
}
fn decode<T: serde::de::DeserializeOwned>(call: &mut ToolCall) -> Result<T, BackendError> {
    let bytes = bounded_json(&call.arguments)?;
    serde_json::from_slice(&bytes)
        .map_err(|_| InputError::Invalid("invalid transfer arguments").into())
}
fn upload_id(text: &str) -> Result<[u8; 16], BackendError> {
    if !text
        .bytes()
        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(InputError::Invalid("upload_id must be lowercase hexadecimal").into());
    }
    Ok(parse_id(text)?)
}
impl<T: ClientTransport> Backend<T> {
    pub(super) fn transfer(
        &self,
        runtime: &Runtime,
        call: &mut ToolCall,
        cancel: &mut oneshot::Receiver<()>,
    ) -> Result<(&'static str, OperationOutput), BackendError> {
        if call.tool == "artifact.download" {
            return self.artifact_download(runtime, decode(call)?, cancel);
        }
        let store = self.uploads.as_ref().ok_or(BackendError::Configuration)?;
        let mut journal = match call.tool.as_str() {
            "upload.begin" => {
                let input: Begin = decode(call)?;
                let class = match input.class.as_str() {
                    "document" => ContentClass::Document,
                    "evidence" => ContentClass::Evidence,
                    "checkpoint" => ContentClass::Checkpoint,
                    _ => return Err(InputError::Invalid("content class").into()),
                };
                store.begin(
                    self.context,
                    UploadSpec {
                        upload: upload_id(&input.upload_id)?,
                        length: input.length,
                        digest: parse_hash(&input.digest)?,
                        class,
                    },
                    RouteEpoch(1),
                )?
            }
            "upload.append" => {
                let input: Append = decode(call)?;
                let bytes = hex_bytes(&input.bytes_hex)?;
                let mut journal = store.open_upload(upload_id(&input.upload_id)?, &self.context)?;
                journal.stage(input.offset, &bytes)?;
                journal
            }
            "upload.seal" | "upload.cancel" => {
                let input: Identity = decode(call)?;
                let mut journal = store.open_upload(upload_id(&input.upload_id)?, &self.context)?;
                if call.tool == "upload.cancel" {
                    journal.cancel()?;
                }
                if call.tool == "upload.seal"
                    && journal.progress().staged != journal.progress().length
                {
                    return Err(TransferError::Incomplete.into());
                }
                journal
            }
            _ => return Err(BackendError::Configuration),
        };
        self.drive_upload(runtime, &mut journal, cancel, call.tool == "upload.seal")?;
        let condition = if journal.progress().cancelled {
            "Cancelled"
        } else if journal.reference().is_some() {
            "Sealed"
        } else {
            "Uploading"
        };
        Ok((
            condition,
            OperationOutput::Upload {
                progress: journal.progress().clone(),
            },
        ))
    }
    fn drive_upload(
        &self,
        runtime: &Runtime,
        journal: &mut UploadJournal,
        cancel: &mut oneshot::Receiver<()>,
        seal: bool,
    ) -> Result<(), BackendError> {
        let start = Instant::now();
        for _ in 0..1028 {
            if cancelled(cancel) {
                return Err(BackendError::Cancelled);
            }
            let Some(request) = journal.next_request()? else {
                return Ok(());
            };
            // Upload Append records transfer progress, but only an explicit Seal
            // call crosses the custody gate and returns a ContentRef.
            if !seal
                && matches!(
                    request.operation,
                    Operation::Upload(UploadRequest::Seal { .. })
                )
            {
                return Ok(());
            }
            let remaining = Duration::from_secs(30).saturating_sub(start.elapsed());
            if remaining.is_zero() {
                return Err(ClientError::OutcomeUnknown {
                    request: Box::new(request),
                }
                .into());
            }
            let reply = std::panic::catch_unwind(AssertUnwindSafe(|| {
                runtime.block_on(async {
                    tokio::select! {
                        result = tokio::time::timeout(remaining, self.client.upload(request.clone())) => {
                            result.map_err(|_| BackendError::Client(ClientError::OutcomeUnknown { request: Box::new(request.clone()) }))?.map_err(BackendError::Client)
                        }
                        _ = &mut *cancel => Err(BackendError::Cancelled),
                    }
                })
            })).map_err(|_| BackendError::Client(ClientError::Transport))??;
            journal.record_reply(&request, &reply)?;
        }
        Err(TransferError::Capacity.into())
    }
    fn artifact_download(
        &self,
        runtime: &Runtime,
        input: Download,
        cancel: &mut oneshot::Receiver<()>,
    ) -> Result<(&'static str, OperationOutput), BackendError> {
        let id = ArtifactId(parse_id(&input.id)?);
        if input.max_bytes == 0
            || input.max_bytes > chunk_limit()
            || (input.offset != 0 && input.token.is_none())
            || input.token.is_some_and(|token| {
                token.ledger != self.context.ledger || token.route_epoch.0 == 0
            })
        {
            return Err(InputError::Invalid("payload read scope or chunk bound").into());
        }
        let mut request = envelope(
            self.context.ledger,
            Operation::Read(ReadRequest {
                consistency: input
                    .token
                    .map(ReadConsistency::Exact)
                    .unwrap_or(ReadConsistency::Linearizable),
                query: ReadQuery::Objects(vec![ObjectRef {
                    ledger: self.context.ledger,
                    kind: ObjectKind::Artifact,
                    id: ObjectId(id.0),
                }]),
                max_items: 1,
            }),
        )?;
        if let Some(token) = input.token {
            request.route_epoch = token.route_epoch;
        }
        std::panic::catch_unwind(AssertUnwindSafe(|| {
            runtime.block_on(async {
                let operation = async {
                    let page = self.client.read(request).await?;
                    if page.objects.is_empty() { return Err(BackendError::NotFound); }
                    if page.next.is_some() || page.objects.len() != 1 { return Err(ClientError::InvalidResponse.into()); }
                    let Some(ReadObject::Artifact { id: found, value }) = page.objects.into_iter().next() else {
                        return Err(ClientError::InvalidResponse.into());
                    };
                    if found != id || value.content().ledger != self.context.ledger
                        || value.content().schema != 1 || value.lifecycle().created > page.token.sequence
                        || value.content().content_hash().map_err(|_| InputError::Capacity)? != value.content_hash() {
                        return Err(ClientError::InvalidResponse.into());
                    }
                    let chunk = match &value.content().payload {
                        ArtifactPayload::Inline(bytes) => {
                            let start = usize::try_from(input.offset).map_err(|_| InputError::Capacity)?;
                            let through = start.checked_add(input.max_bytes as usize).ok_or(InputError::Capacity)?.min(bytes.len());
                            let selected = bytes.get(start..through).ok_or(InputError::Invalid("payload offset"))?;
                            let mut copied = Vec::new();
                            copied.try_reserve_exact(selected.len()).map_err(|_| InputError::Capacity)?;
                            copied.extend_from_slice(selected);
                            ContentChunk { offset: input.offset, eof: through == bytes.len(), bytes: copied }
                        }
                        ArtifactPayload::Content(reference) => {
                            if reference.domain.0 != self.context.ledger.tenant.0 || input.offset > reference.length {
                                return Err(ClientError::InvalidResponse.into());
                            }
                            let mut request = envelope(self.context.ledger, Operation::Download {
                                content: reference.clone(), offset: input.offset, max_bytes: input.max_bytes
                            })?;
                            request.route_epoch = page.token.route_epoch;
                            self.client.download(request).await?
                        }
                    };
                    Ok(("Read", OperationOutput::ArtifactPayload { artifact: id, token: page.token, content_hash: value.content_hash(), chunk }))
                };
                tokio::select! {
                    result = tokio::time::timeout(Duration::from_secs(30), operation) => result.map_err(|_| BackendError::Client(ClientError::Transport))?,
                    _ = cancel => Err(BackendError::Cancelled),
                }
            })
        })).map_err(|_| BackendError::Client(ClientError::Transport))?
    }
}
fn hex_bytes(value: &str) -> Result<Vec<u8>, BackendError> {
    if value.is_empty() || !value.len().is_multiple_of(2) || value.len() > TRANSFER_CHUNK_BYTES * 2
    {
        return Err(InputError::Capacity.into());
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(value.len() / 2)
        .map_err(|_| InputError::Capacity)?;
    for pair in value.as_bytes().chunks_exact(2) {
        let high = pair
            .first()
            .copied()
            .and_then(digit)
            .ok_or(InputError::Invalid("payload hexadecimal"))?;
        let low = pair
            .get(1)
            .copied()
            .and_then(digit)
            .ok_or(InputError::Invalid("payload hexadecimal"))?;
        bytes.push(
            high.checked_mul(16)
                .and_then(|n| n.checked_add(low))
                .ok_or(InputError::Invalid("payload hexadecimal"))?,
        );
    }
    Ok(bytes)
}
fn digit(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => value.checked_sub(b'0'),
        b'a'..=b'f' => value.checked_sub(b'a')?.checked_add(10),
        b'A'..=b'F' => value.checked_sub(b'A')?.checked_add(10),
        _ => None,
    }
}
