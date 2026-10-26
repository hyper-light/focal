mod args;
mod authored;
pub(crate) mod bus;
mod claim_wait;
pub(crate) mod cluster;
pub(crate) mod command_tree;
pub(crate) mod context;
pub(crate) mod deployment;
pub(crate) mod discovery;
mod documents;
mod download;
pub(super) mod errors;
pub(crate) mod grammar;
mod graph;
mod lifecycle;
pub(crate) mod look;
mod managed;
mod mcp;
mod monitor;
mod native;
mod native_documents;
mod output;
mod reads;
mod reconcile;
mod request_files;
pub(crate) mod startup;
#[cfg(test)]
mod tests;
pub(crate) mod trace;
mod upload;
mod upload_control;
mod validators;
mod watch;
pub(crate) use args::OutputFormat;
use args::*;
pub(super) use args::{Commands, RequestArgs, RequestCommand};
pub(super) fn check_request_file(path: &std::path::Path) -> Result<()> {
    request_files::check(path)
}
use focal_client::{Client, ClientError, LocalTransport, RetryPolicy, input::*, pending::*};
use focal_model::*;
use focal_node::config::Settings;
use focal_wire::*;
pub(super) use mcp::{CodeCommand, code, serve};
use std::{io::Write, path::PathBuf};

type Result<T> = std::result::Result<T, CliError>;
fn validation_context_error(
    error: focal_client::validation_context::ValidationContextError,
) -> CliError {
    use focal_client::validation_context::ValidationContextError;
    match error {
        ValidationContextError::Client(error) => CliError::Client(error),
        ValidationContextError::NotFound => CliError::NotFound,
        ValidationContextError::InvalidRequest => {
            CliError::Input("invalid validation context query".into())
        }
        ValidationContextError::Capacity => CliError::Document(InputError::Capacity),
    }
}
#[derive(Debug, thiserror::Error)]
pub(super) enum CliError {
    #[error("{0}")]
    Input(String),
    /// A bound of this client's own reached, named with what holds it.
    #[error("{0}")]
    Capacity(String),
    #[error(transparent)]
    Document(#[from] InputError),
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error(transparent)]
    Pending(#[from] PendingError),
    #[error(transparent)]
    Managed(#[from] focal_client::managed_store::ManagedStoreError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("object not found at the observed ledger prefix")]
    NotFound,
    /// A code-mode program ended without returning (19 §Code mode): it threw
    /// or met a bound; its result, with every call it made, is on stdout.
    #[error("the program ended {0}; its result and calls were written to stdout")]
    Program(String),
    #[error("more than one claim matches; use list claims or an exact ID")]
    Ambiguous,
    #[error("singular claim selection is incomplete; narrow the filters or use list claims")]
    Incomplete,
    #[error("claim wait ended {0:?}; latest observed state was written to stdout")]
    WaitUnfinished(focal_client::claim_wait::ClaimWaitCondition),
    #[error("invalid or inconsistent server response")]
    InvalidResponse,
    #[error("mutation did not commit: {0:?}; its exact request remains journaled")]
    Domain(DomainOutcome),
    #[error("mutation has no verified durable receipt; retry the saved operation")]
    Unconfirmed,
    #[error("the native owner refused the frame: {}", .0.detail)]
    NativeRefused(NativeRefusal),
    #[error(transparent)]
    NativeCompile(#[from] focal_native_client::CompileError),
    #[error(transparent)]
    NativeStore(#[from] focal_client::native_store::NativeStoreError),
    #[error(transparent)]
    Other(Box<dyn std::error::Error + Send + Sync>),
}
impl From<focal_client::operations::EngineError> for CliError {
    fn from(error: focal_client::operations::EngineError) -> Self {
        Self::Input(error.to_string())
    }
}
impl From<focal_native_client::DriveError> for CliError {
    fn from(error: focal_native_client::DriveError) -> Self {
        use focal_native_client::DriveError;
        match error {
            DriveError::Compile(error) => Self::NativeCompile(error),
            DriveError::Store(error) => Self::NativeStore(error),
            DriveError::Client(error) => Self::Client(error),
            DriveError::Input(error) => Self::Document(error),
            DriveError::ProjectionOnly | DriveError::Cancelled => Self::Input(error.to_string()),
        }
    }
}
pub(super) struct Context {
    client: Client<context::Transport>,
    build: BuildContext,
    operation: OperationContext,
    root: PathBuf,
    admin_root: Option<PathBuf>,
    invocation: Option<String>,
}
impl Context {
    fn open(settings: &Settings, selection: Option<&str>) -> Result<Self> {
        match context::selected(settings, selection)? {
            Some((profile, history, name)) => {
                let mut context = context::connect(settings, profile, history)?;
                context.invocation = Self::recovery_invocation(
                    &settings
                        .data_dir()
                        .map_err(|e| CliError::Other(Box::new(e)))?,
                    &name,
                );
                Ok(context)
            }
            None => Self::open_local(settings),
        }
    }
    fn open_local(settings: &Settings) -> Result<Self> {
        let root = settings
            .data_dir()
            .map_err(|e| CliError::Other(Box::new(e)))?;
        let identity = focal_node::node_files::read_identity(settings, &root)
            .map_err(|e| CliError::Other(Box::new(e)))?;
        // NETWORK also proves joined identity if its private JOIN files are
        // lost. Never fall back to the founder's issuer in that case.
        // The data directory's keys where it has them, made nowhere: this command runs beside the
        // node that owns them.
        let keys = settings
            .found_data_keys(&root)
            .map_err(|e| CliError::Other(Box::new(e)))?;
        let actor = focal_node::network_join::local_unix_principal(
            &root,
            keys.as_ref(),
            &identity,
            context::now()?,
        )
        .map_err(|error| CliError::Input(format!("invalid local node client context: {error}")))?;
        let limits = WireLimits::default();
        let transport = LocalTransport::connect(
            root.join(focal_node::network_admin::LOCAL_SOCKET),
            limits.clone(),
        )
        .map_err(|e| CliError::Other(Box::new(e)))?;
        let client = Client::new(
            context::Transport::Unix(transport),
            RetryPolicy::default(),
            limits,
            1,
        )?;
        let client = match trace::sink() {
            Some(sink) => client.with_trace(sink),
            None => client,
        };
        Ok(Self {
            client,
            build: BuildContext {
                ledger: identity.ledger,
                actor,
                root: identity.root,
                policy_revision: 1,
            },
            operation: OperationContext {
                cluster: identity.cluster,
                principal: actor,
                ledger: identity.ledger,
            },
            invocation: Self::recovery_invocation(&root, "local"),
            admin_root: Some(root.clone()),
            root,
        })
    }
    fn recovery_invocation(root: &std::path::Path, selection: &str) -> Option<String> {
        let root = if root.is_absolute() {
            root.to_owned()
        } else {
            std::env::current_dir().ok()?.join(root)
        };
        let path = root
            .to_str()
            .filter(|path| !path.chars().any(char::is_control))?;
        Some(format!(
            "focal --data-dir '{}' --client-context '{}'",
            path.replace('\'', "'\\''"),
            selection.replace('\'', "'\\''")
        ))
    }
    fn envelope(&self, operation: Operation) -> Result<RequestEnvelope> {
        Ok(RequestEnvelope {
            protocol: participant_protocol(&operation),
            ledger: self.build.ledger,
            route_epoch: RouteEpoch(1),
            request_epoch: RequestEpoch(1),
            request_id: RequestId(random_id()?),
            operation,
        })
    }
}
/// Ctrl-C, awaited beside a command's own waits: a follow or a list it ends
/// stops at a page boundary with what was flushed intact.
pub(super) struct Interrupt(hyper_rt::signal::SignalStream);
impl Interrupt {
    pub(super) fn subscribe() -> std::io::Result<Self> {
        hyper_rt::signal::subscribe(&[hyper_rt::signal::Signal::Interrupt])
            .map(Self)
            .map_err(std::io::Error::other)
    }
    /// Waits for Ctrl-C; an error when no wait could be kept.
    pub(super) async fn wait(&self) -> std::io::Result<()> {
        self.0
            .recv()
            .await
            .map(|_| ())
            .map_err(std::io::Error::other)
    }
}
/// Buffers of the streaming commands' standard output, and their size: a
/// page is written a chunk at a time, at most this much in flight.
const STDOUT_BUFFERS: usize = 4;
const STDOUT_CHUNK: usize = 64 * 1024;
/// The process's standard output for a streaming command, written by the
/// runtime's writer thread: a write waiting on a slow reader is raced
/// against Ctrl-C like every other wait. Taken once in a process.
pub(super) fn streaming_stdout() -> std::io::Result<hyper_rt::stdio::Stdout> {
    hyper_rt::stdio::stdout(STDOUT_BUFFERS, STDOUT_CHUNK).map_err(std::io::Error::other)
}
fn random_id() -> std::result::Result<[u8; 16], InputError> {
    let mut bytes = [0; 16];
    getrandom::fill(&mut bytes).map_err(|_| InputError::Identity)?;
    if bytes == [0; 16] {
        return Err(InputError::Identity);
    }
    Ok(bytes)
}
pub(super) async fn run(
    settings: &Settings,
    command: Commands,
    selection: Option<&str>,
) -> Result<()> {
    let context = Context::open(settings, selection)?;
    if let focal_client::operations::Engine::Native(standing) = native::detect(&context).await? {
        return native::run(&context, command, &standing).await;
    }
    let command = match command {
        Commands::Claim {
            command: ClaimCommand::Wait(args),
        } => return claim_wait::run(&context, args).await,
        Commands::Monitor {
            command: monitor::MonitorCommand::Get(args),
        } => return monitor::get(&context, args).await,
        Commands::Watch { command } => {
            return watch::run(&context, command, focal_client::watch::WatchEngine::Legacy).await;
        }
        Commands::Ledger { command } => return graph::run(&context, command).await,
        Commands::Validator { command } => return validators::run(&context, command).await,
        Commands::Artifact {
            command: ArtifactCommand::Upload { command },
        } => return upload_control::run(&context, command).await,
        Commands::Get { command } => return reads::get(&context, command).await,
        Commands::List { command } => return reads::list(&context, command).await,
        Commands::Artifact {
            command: ArtifactCommand::Register(args),
        } if args
            .payload_file
            .as_ref()
            .is_some_and(|path| path != std::path::Path::new("-")) =>
        {
            return upload::register(&context, *args).await;
        }
        Commands::Submit {
            command: SubmitCommand::Artifact(args),
        } if args
            .payload_file
            .as_ref()
            .is_some_and(|path| path != std::path::Path::new("-")) =>
        {
            return upload::artifact(&context, args).await;
        }
        command => command,
    };
    let (authored, options) = authored::mutation(command)?;
    if options.operation.is_none() {
        return managed::submit(&context, authored, options).await;
    }
    let focal_client::operations::PlannedOperation::Mutation(command) =
        authored.build(&context.build, &mut random_id)?
    else {
        return Err(CliError::Input("mutation builder returned a read".into()));
    };
    submit(&context, command, options).await
}
async fn submit(context: &Context, command: Command, options: MutationOptions) -> Result<()> {
    let mut request = context.envelope(Operation::Submit {
        expected_revision: options.expected_revision.map(ObjectRevision),
        command,
    })?;
    if let Operation::Submit {
        command,
        expected_revision,
    } = &mut request.operation
        && expected_revision.is_none()
        && matches!(
            command,
            Command::AcknowledgeTestament { .. }
                | Command::BeginWholeWorkValidation { .. }
                | Command::BeginIncrementValidation { .. }
                | Command::CompleteWholeWork { .. }
        )
    {
        let claim = command
            .claim_id()
            .ok_or(InputError::Invalid("claim is required"))?;
        *expected_revision = Some(
            context
                .client
                .claim_revision(context.build.ledger, claim, request.request_id)
                .await?
                .ok_or(InputError::Invalid("claim was not found"))?,
        );
    }
    let epoch = context.envelope(Operation::OpenEpoch {
        epoch: RequestEpoch(1),
    })?;
    let path = match options.operation {
        Some(path) => path,
        None => {
            let parent = context.root.join("client").join("operations");
            private_parent(&parent)?;
            parent.join(request.request_id.to_string())
        }
    };
    let mut journal = OperationJournal::create(&path, context.operation, epoch, request)?;
    writeln!(std::io::stderr().lock(), "operation: {}", path.display())?;
    drive(context, &mut journal, options.output.format).await
}
fn private_parent(path: &std::path::Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(path)?;
        let metadata = std::fs::symlink_metadata(path)?;
        if !metadata.is_dir() || metadata.permissions().mode() & 0o077 != 0 {
            return Err(CliError::Input(
                "client operation parent must be a private directory".into(),
            ));
        }
    }
    #[cfg(not(unix))]
    {
        if !path.try_exists()? {
            if let Some(parent) = path.parent()
                && !parent.as_os_str().is_empty()
            {
                std::fs::create_dir_all(parent)?;
            }
            // Another process may make it after the look; its owner is checked below.
            focal_platform::fs::ensure_dir_private(path)?;
        }
        let owner = focal_platform::fs::current_owner()?;
        match focal_platform::fs::private_dir_owner(path)? {
            Some(found) if found == owner => {}
            _ => {
                return Err(CliError::Input(
                    "client operation parent must be a private directory".into(),
                ));
            }
        }
    }
    Ok(())
}
async fn drive(
    context: &Context,
    journal: &mut OperationJournal,
    format: OutputFormat,
) -> Result<()> {
    while let Some(request) = journal.next_request()?.cloned() {
        let reply = match context.client.submit(request).await {
            Ok(reply) => reply,
            Err(error) => {
                let condition = if matches!(error, ClientError::OutcomeUnknown { .. }) {
                    "OutcomeUnknown"
                } else {
                    "RequestUnconfirmed"
                };
                output::unconfirmed(journal, condition, None, format)?;
                return Err(error.into());
            }
        };
        match &reply {
            MutationReply::Committed(_) | MutationReply::Domain(DomainOutcome::Duplicate(_)) => {
                journal.record_reply(&reply)?;
            }
            MutationReply::Domain(outcome) => {
                output::unconfirmed(journal, "DomainOutcome", Some(&reply), format)?;
                return Err(CliError::Domain(outcome.clone()));
            }
            _ => {
                output::unconfirmed(journal, "RequestUnconfirmed", Some(&reply), format)?;
                return Err(CliError::Unconfirmed);
            }
        }
    }
    output::journal(journal, format)
}
pub(super) async fn status(settings: &Settings, selection: Option<&str>) -> Result<()> {
    let context = Context::open(settings, selection)?;
    if native::detect(&context).await?.standing().is_some() {
        return native::status(&context, OutputFormat::Json).await;
    }
    let request = context.envelope(Operation::Read(ReadRequest {
        consistency: ReadConsistency::Linearizable,
        query: ReadQuery::Objects(Vec::new()),
        max_items: 1,
    }))?;
    let reply = context.client.request(request).await?;
    super::output_response(reply).map_err(CliError::Other)
}

/// `validate document` under the selected context: the engine is probed the
/// way every mutation probes it, then the document is checked the way that
/// engine's mutation path checks it. Nothing is journaled or sent.
pub(super) async fn schema_validate(
    settings: &Settings,
    selection: Option<&str>,
    operation: &str,
    input: DocumentInput,
    native: bool,
) -> Result<()> {
    let context = Context::open(settings, selection)?;
    discovery::validate_online(&context, operation, input, native).await
}
pub(super) async fn request(
    settings: &Settings,
    args: RequestArgs,
    selection: Option<&str>,
) -> Result<()> {
    match (args.file, args.command) {
        (None, Some(RequestCommand::Build(args))) => {
            request_files::build(settings, selection, args)
        }
        (None, Some(RequestCommand::Check { file })) => request_files::check(&file),
        (None, Some(RequestCommand::Send { file })) => {
            request_files::send(settings, selection, &file).await
        }
        (Some(file), None) => request_files::send(settings, selection, &file).await,
        (
            None,
            Some(RequestCommand::Retry {
                operation: Some(operation),
                operation_id: None,
                output,
            }),
        ) => {
            let context = Context::open(settings, selection)?;
            if upload::resume_legacy(&context, &operation, output.format).await? {
                return Ok(());
            }
            let mut journal = OperationJournal::open(&operation, &context.operation)?;
            writeln!(
                std::io::stderr().lock(),
                "operation: {}",
                operation.display()
            )?;
            drive(&context, &mut journal, output.format).await
        }
        (
            None,
            Some(RequestCommand::Inspect {
                operation,
                operation_id: None,
                remote,
                output,
            }),
        ) => {
            let context = Context::open(settings, selection)?;
            let journal = OperationJournal::open(
                operation.ok_or_else(|| CliError::Input("operation path is required".into()))?,
                &context.operation,
            )?;
            if remote {
                reconcile::inspect(&context, &journal, output.format).await
            } else {
                output::journal(&journal, output.format)
            }
        }
        (
            None,
            Some(RequestCommand::Retry {
                operation: None,
                operation_id: Some(id),
                output,
            }),
        ) => {
            let context = Context::open(settings, selection)?;
            if let Some(native) = id.strip_prefix("n1:") {
                let id = format!("n1:{native}").parse()?;
                return native::retry(&context, id, output.format).await;
            }
            managed::retry(&context, &id, output.format).await
        }
        (
            None,
            Some(RequestCommand::Inspect {
                operation: None,
                operation_id: Some(id),
                remote,
                output,
            }),
        ) => {
            let context = Context::open(settings, selection)?;
            if id.starts_with("n1:") {
                return native::inspect(&context, id.parse()?, remote, output.format).await;
            }
            managed::inspect(&context, &id, remote, output.format).await
        }
        (None, Some(RequestCommand::Reserve { output })) => {
            managed::reserve(&Context::open(settings, selection)?, output.format).await
        }
        (None, Some(RequestCommand::Pending { output })) => {
            managed::pending(&Context::open(settings, selection)?, output.format)
        }
        (
            None,
            Some(RequestCommand::Acknowledge {
                operation_id,
                output,
            }),
        ) => {
            managed::acknowledge(
                &Context::open(settings, selection)?,
                &operation_id,
                output.format,
            )
            .await
        }
        (
            None,
            Some(RequestCommand::Seal {
                operation_id,
                output,
            }),
        ) => {
            managed::seal(
                &Context::open(settings, selection)?,
                &operation_id,
                output.format,
            )
            .await
        }
        (
            None,
            Some(RequestCommand::Status {
                request_id,
                epoch,
                output,
            }),
        ) => {
            let query =
                focal_client::operations::RequestStatusDocument { epoch, request_id }.build()?;
            let context = Context::open(settings, selection)?;
            reconcile::query(&context, query, output.format).await
        }
        (None, Some(RequestCommand::Epoch { epoch, output })) => {
            let query = focal_client::operations::RequestEpochDocument { epoch }.build()?;
            let context = Context::open(settings, selection)?;
            reconcile::query(&context, query, output.format).await
        }
        _ => Err(CliError::Input(
            "provide a wire request file or a request subcommand".into(),
        )),
    }
}
