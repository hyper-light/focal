//! One CLI owner stages immutable source bytes before any upload or artifact
//! request. A saved continuation bridges custody completion to normal managed
//! or legacy request preparation, including a crash between those two steps.
use super::*;
use focal_client::{
    artifact_transfer::{
        MAX_TRANSFER_BYTES, TRANSFER_CHUNK_BYTES, TransferError, UploadJournal, UploadSpec,
        UploadStore, UploadStoreLimits, digest_reader,
    },
    managed_store::ManagedOperationId,
    operations::{AuthoredOperation, PlannedOperation},
};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    io::{Read, Seek, SeekFrom, Write},
    panic::AssertUnwindSafe,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

const STORE: &str = "CLI.uploads";
#[derive(PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Attachment {
    version: u16,
    operation_id: Option<String>,
    operation_path: Option<Vec<u8>>,
    expected_revision: Option<u64>,
    document: Document,
    source: Vec<u8>,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum Document {
    Attach(ArtifactDocument),
    Register(RegisterArtifactDocument),
}
impl Document {
    fn authored(self) -> AuthoredOperation {
        match self {
            Self::Attach(value) => AuthoredOperation::ArtifactSubmit(value),
            Self::Register(value) => AuthoredOperation::ArtifactRegister(value),
        }
    }
    fn payload_mut(&mut self) -> &mut PayloadDocument {
        match self {
            Self::Attach(value) => &mut value.payload,
            Self::Register(value) => &mut value.payload,
        }
    }
}
fn decode_attachment(bytes: &[u8]) -> Result<Attachment> {
    #[derive(Deserialize)]
    struct Version {
        version: u16,
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct V1 {
        version: u16,
        operation_id: Option<String>,
        operation_path: Option<Vec<u8>>,
        expected_revision: Option<u64>,
        document: ArtifactDocument,
        source: Vec<u8>,
    }
    let version: Version =
        serde_json::from_slice(bytes).map_err(|_| other(TransferError::Corrupt))?;
    match version.version {
        1 => {
            let old: V1 =
                serde_json::from_slice(bytes).map_err(|_| other(TransferError::Corrupt))?;
            if old.version != 1 {
                return Err(other(TransferError::Corrupt));
            }
            Ok(Attachment {
                version: 2,
                operation_id: old.operation_id,
                operation_path: old.operation_path,
                expected_revision: old.expected_revision,
                document: Document::Attach(old.document),
                source: old.source,
            })
        }
        2 => serde_json::from_slice(bytes).map_err(|_| other(TransferError::Corrupt)),
        _ => Err(other(TransferError::Corrupt)),
    }
}
struct Source {
    path: PathBuf,
    file: File,
    length: u64,
}
fn source(path: PathBuf) -> Result<Source> {
    let file = File::open(&path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(CliError::Input(
            "payload file must be a regular file".into(),
        ));
    }
    if metadata.len() > MAX_TRANSFER_BYTES {
        return Err(other(TransferError::Capacity));
    }
    Ok(Source {
        path,
        file,
        length: metadata.len(),
    })
}

fn other(error: TransferError) -> CliError {
    match error {
        TransferError::Client(error) => CliError::Client(error),
        TransferError::Io(error) => CliError::Io(error),
        other => CliError::Other(Box::new(other)),
    }
}
fn store(context: &Context) -> Result<UploadStore> {
    UploadStore::bootstrap(
        &context.root,
        STORE,
        context.operation,
        UploadStoreLimits::default(),
    )
    .map_err(other)
}
fn existing(context: &Context) -> Result<Option<UploadStore>> {
    for name in [STORE, "CLI.uploads.lock", "CLI.uploads.initialized"] {
        match fs::symlink_metadata(context.root.join(name)) {
            Ok(_) => return store(context).map(Some),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(None)
}
pub(super) fn artifact(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    mut args: ArtifactArgs,
) -> Result<()> {
    let path = args
        .payload_file
        .take()
        .ok_or_else(|| CliError::Input("payload file required".into()))?;
    let source = source(path)?;
    if source.length <= 16 * 1024 {
        args.payload_file = Some(source.path);
        let (document, options) = documents::artifact(args)?;
        return submit(runtime, context, Document::Attach(document), options);
    }
    // Parse every authored field before reserving identity or admitting bytes.
    args.text = Some(String::new());
    let (mut document, options) = documents::artifact(args)?;
    document.payload = PayloadDocument::Inline { bytes: Vec::new() };
    large(
        runtime,
        context,
        source,
        Document::Attach(document),
        options,
    )
}
pub(super) fn register(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    mut args: RegisterArtifactArgs,
) -> Result<()> {
    let path = args
        .payload_file
        .take()
        .ok_or_else(|| CliError::Input("payload file required".into()))?;
    let source = source(path)?;
    if source.length <= 16 * 1024 {
        args.payload_file = Some(source.path);
        let (authored, options) = lifecycle::register(args)?;
        let AuthoredOperation::ArtifactRegister(document) = authored else {
            return Err(CliError::InvalidResponse);
        };
        return submit(runtime, context, Document::Register(document), options);
    }
    args.text = Some(String::new());
    let (authored, options) = lifecycle::register(args)?;
    let AuthoredOperation::ArtifactRegister(mut document) = authored else {
        return Err(CliError::InvalidResponse);
    };
    document.payload = PayloadDocument::Inline { bytes: Vec::new() };
    large(
        runtime,
        context,
        source,
        Document::Register(document),
        options,
    )
}
fn large(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    mut source: Source,
    document: Document,
    mut options: MutationOptions,
) -> Result<()> {
    document.clone().authored().preflight(&context.build)?;
    let digest = digest_reader(&mut source.file, source.length).map_err(other)?;
    let identity = if let Some(path) = &options.operation {
        Identity::Legacy(path_bytes(path))
    } else {
        Identity::Managed(managed::reserve_for_submission(
            runtime,
            context,
            &mut options,
        )?)
    };
    let attachment = Attachment {
        version: 2,
        operation_id: options.operation_id.clone(),
        operation_path: options.operation.as_ref().map(|path| path_bytes(path)),
        expected_revision: options.expected_revision,
        document,
        source: path_bytes(&source.path),
    };
    let bytes =
        serde_json::to_vec(&attachment).map_err(|error| CliError::Other(Box::new(error)))?;
    if bytes.len() > MAX_INPUT_BYTES {
        return Err(InputError::Capacity.into());
    }
    let result = (|| {
        let store = store(context)?;
        let mut journal = store
            .begin(
                context.operation,
                UploadSpec {
                    upload: identity.upload(context),
                    class: ContentClass::Evidence,
                    length: source.length,
                    digest,
                },
                RouteEpoch(1),
            )
            .map_err(other)?;
        bind_attachment(&mut journal, &attachment, &bytes)?;
        stage_source(&mut journal, &mut source.file)
            .and_then(|()| finish(runtime, context, journal, attachment, options.output.format))
    })();
    let (document, options) = report(context, &identity, options.output.format, result)?;
    submit(runtime, context, document, options)
}
fn bind_attachment(
    journal: &mut UploadJournal,
    attachment: &Attachment,
    bytes: &[u8],
) -> Result<()> {
    if let Some(previous) = journal.intent() {
        if decode_attachment(previous)? != *attachment {
            return Err(other(TransferError::Conflict));
        }
        Ok(())
    } else {
        journal.bind_intent(bytes).map_err(other)
    }
}

fn stage_source(journal: &mut UploadJournal, source: &mut File) -> Result<()> {
    let spec = journal.spec();
    if source.metadata()?.len() != spec.length
        || digest_reader(source, spec.length).map_err(other)? != spec.digest
    {
        return Err(other(TransferError::Conflict));
    }
    let mut offset = journal.progress().staged;
    source.seek(SeekFrom::Start(offset))?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(TRANSFER_CHUNK_BYTES)
        .map_err(|_| InputError::Capacity)?;
    bytes.resize(TRANSFER_CHUNK_BYTES, 0);
    while offset < spec.length {
        let count = usize::try_from(
            spec.length
                .checked_sub(offset)
                .ok_or(InputError::Capacity)?
                .min(TRANSFER_CHUNK_BYTES as u64),
        )
        .map_err(|_| InputError::Capacity)?;
        let bytes = bytes.get_mut(..count).ok_or(InputError::Capacity)?;
        source.read_exact(bytes)?;
        offset = journal.stage(offset, bytes).map_err(other)?;
    }
    Ok(())
}
fn finish(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    mut journal: UploadJournal,
    mut attachment: Attachment,
    format: OutputFormat,
) -> Result<(Document, MutationOptions)> {
    if journal.progress().cancelled {
        return Err(other(TransferError::Cancelled));
    }
    if journal.progress().staged != journal.progress().length {
        let mut source = File::open(path_from_bytes(attachment.source.clone())?)?;
        stage_source(&mut journal, &mut source)?;
    }
    let start = Instant::now();
    for _ in 0..1028 {
        let Some(request) = journal.next_request().map_err(other)? else {
            break;
        };
        let remaining = Duration::from_secs(30).saturating_sub(start.elapsed());
        if remaining.is_zero() {
            return Err(ClientError::OutcomeUnknown {
                request: Box::new(request),
            }
            .into());
        }
        let reply = std::panic::catch_unwind(AssertUnwindSafe(|| {
            runtime.block_on(async {
                tokio::time::timeout(remaining, context.client.upload(request.clone()))
                    .await
                    .map_err(|_| ClientError::OutcomeUnknown {
                        request: Box::new(request.clone()),
                    })?
            })
        }))
        .map_err(|_| ClientError::Transport)??;
        journal.record_reply(&request, &reply).map_err(other)?;
    }
    let reference = journal
        .reference()
        .cloned()
        .ok_or_else(|| other(TransferError::Incomplete))?;
    *attachment.document.payload_mut() = PayloadDocument::Content {
        reference: ContentReferenceDocument {
            domain: reference.domain.to_string(),
            root: reference.root.to_string(),
            length: reference.length,
            class: match reference.class {
                ContentClass::Document => "document",
                ContentClass::Evidence => "evidence",
                ContentClass::Checkpoint => "checkpoint",
            }
            .into(),
        },
    };
    let options = MutationOptions {
        operation: attachment.operation_path.map(path_from_bytes).transpose()?,
        operation_id: attachment.operation_id,
        expected_revision: attachment.expected_revision,
        output: OutputOptions { format },
    };
    drop(journal);
    Ok((attachment.document, options))
}
fn submit(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    document: Document,
    options: MutationOptions,
) -> Result<()> {
    let authored = document.authored();
    if options.operation.is_none() {
        return managed::submit(runtime, context, authored, options);
    }
    let PlannedOperation::Mutation(command) = authored.build(&context.build, &mut random_id)?
    else {
        return Err(CliError::InvalidResponse);
    };
    super::submit(runtime, context, command, options)
}
enum Identity {
    Managed(ManagedOperationId),
    Legacy(Vec<u8>),
}
impl Identity {
    fn upload(&self, context: &Context) -> [u8; 16] {
        let mut hash = blake3::Hasher::new();
        hash.update(b"focal.cli.artifact-upload.v1\0");
        hash.update(&context.operation.cluster);
        hash.update(&context.operation.ledger.tenant.0);
        hash.update(&context.operation.ledger.session.0);
        hash.update(&context.operation.principal.0);
        match self {
            Self::Managed(id) => {
                hash.update(b"managed");
                hash.update(id.to_string().as_bytes());
            }
            Self::Legacy(path) => {
                hash.update(b"legacy");
                hash.update(path);
            }
        }
        let mut result = [0; 16];
        for (to, from) in result.iter_mut().zip(hash.finalize().as_bytes()) {
            *to = *from;
        }
        result
    }
}
pub(super) fn resume_managed(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    id: ManagedOperationId,
    format: OutputFormat,
) -> Result<bool> {
    resume(runtime, context, Identity::Managed(id), format)
}
pub(super) fn resume_legacy(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    path: &Path,
    format: OutputFormat,
) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => return Ok(false),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    resume(runtime, context, Identity::Legacy(path_bytes(path)), format)
}
fn resume(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    identity: Identity,
    format: OutputFormat,
) -> Result<bool> {
    let Some(store) = existing(context)? else {
        return Ok(false);
    };
    let journal = match store.open_upload(identity.upload(context), &context.operation) {
        Ok(journal) => journal,
        Err(TransferError::Invalid) => return Ok(false),
        Err(error) => return Err(other(error)),
    };
    let bytes = journal
        .intent()
        .ok_or_else(|| other(TransferError::Corrupt))?;
    let attachment = decode_attachment(bytes)?;
    let matches = match &identity {
        Identity::Managed(id) => {
            attachment.operation_id.as_deref() == Some(id.to_string().as_str())
                && attachment.operation_path.is_none()
        }
        Identity::Legacy(path) => {
            attachment.operation_id.is_none() && attachment.operation_path.as_ref() == Some(path)
        }
    };
    if attachment.version != 2 || !matches {
        return Err(other(TransferError::Conflict));
    }
    attachment
        .document
        .clone()
        .authored()
        .preflight(&context.build)?;
    let (document, options) = report(
        context,
        &identity,
        format,
        finish(runtime, context, journal, attachment, format),
    )?;
    submit(runtime, context, document, options)?;
    Ok(true)
}
fn report<T>(
    context: &Context,
    identity: &Identity,
    format: OutputFormat,
    result: Result<T>,
) -> Result<T> {
    if let Err(error) = &result {
        if let Identity::Managed(id) = identity {
            if matches!(format, OutputFormat::Json | OutputFormat::Yaml) {
                let value = serde_json::json!({"operation_id":id.to_string(),"condition":"UploadPending","upload_id":format!("{:032x}",u128::from_be_bytes(identity.upload(context))),"error":error.to_string()});
                let mut output = std::io::stdout().lock();
                super::output::structured_to(&mut output, &value, format)?;
                output.flush()?;
            }
            let mut stderr = std::io::stderr().lock();
            if let Some(command) = &context.invocation {
                writeln!(
                    stderr,
                    "Artifact transfer remains saved. Recovery: {command} request retry --operation-id {id}"
                )?;
            } else {
                writeln!(
                    stderr,
                    "Artifact transfer remains saved. Recovery: focal request retry --operation-id {id} (with the same data directory and client context)."
                )?;
            }
        } else {
            writeln!(
                std::io::stderr().lock(),
                "Artifact transfer remains saved; retry the same --operation path with request retry."
            )?;
        }
    }
    result
}
fn path_bytes(path: &Path) -> Vec<u8> {
    focal_platform::path_to_bytes(path)
}
fn path_from_bytes(bytes: Vec<u8>) -> Result<PathBuf> {
    focal_platform::path_from_bytes(&bytes).ok_or_else(|| other(TransferError::Corrupt))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn version_one_attach_binding_retries_without_rewriting_and_cannot_become_registration() {
        let id = "00000000000000000000000000000001";
        let document = serde_json::json!({"claim":id,"receipt":{"id":id,"epoch":1},"evidence_set":id,"kind":"test-report","schema_hash":"01".repeat(32),"payload":{"type":"inline","bytes":[]}});
        let old=serde_json::to_vec(&serde_json::json!({"version":1,"operation_id":null,"operation_path":[47,120],"expected_revision":null,"document":document,"source":[47,121]})).unwrap();
        let mut attachment = decode_attachment(&old).unwrap();
        assert_eq!(attachment.version, 2);
        let root = tempfile::tempdir().unwrap();
        let context = focal_client::pending::OperationContext {
            cluster: [1; 16],
            principal: ParticipantId([2; 16]),
            ledger: LedgerId {
                tenant: TenantId([3; 16]),
                session: SessionId([4; 16]),
            },
        };
        let mut journal = UploadJournal::begin(
            root.path().join("upload"),
            context,
            UploadSpec {
                upload: [5; 16],
                class: ContentClass::Evidence,
                length: 3,
                digest: ContentHash(*blake3::hash(b"abc").as_bytes()),
            },
            RouteEpoch(1),
            focal_client::artifact_transfer::TransferLimits::default(),
        )
        .unwrap();
        journal.bind_intent(&old).unwrap();
        bind_attachment(
            &mut journal,
            &attachment,
            &serde_json::to_vec(&attachment).unwrap(),
        )
        .unwrap();
        assert_eq!(journal.intent(), Some(old.as_slice()));
        let Document::Attach(document) = attachment.document else {
            panic!("old attachment changed family");
        };
        attachment.document = Document::Register(RegisterArtifactDocument {
            id: document.id,
            kind: document.kind,
            schema_hash: document.schema_hash,
            metadata: document.metadata,
            payload: document.payload,
            inputs: document.inputs,
            visibility: document.visibility,
        });
        assert!(
            bind_attachment(
                &mut journal,
                &attachment,
                &serde_json::to_vec(&attachment).unwrap()
            )
            .is_err()
        );
        assert_eq!(journal.intent(), Some(old.as_slice()));
        assert!(decode_attachment(br#"{"version":3}"#).is_err());
    }
}
