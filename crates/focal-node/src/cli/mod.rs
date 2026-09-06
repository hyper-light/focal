mod args;
mod authored;
mod claim_wait;
pub(crate) mod cluster;
pub(crate) mod command_tree;
pub(crate) mod context;
pub(crate) mod discovery;
mod documents;
mod download;
pub(super) mod errors;
mod graph;
mod lifecycle;
mod managed;
mod mcp;
mod monitor;
mod output;
mod reads;
mod reconcile;
mod request_files;
#[cfg(test)]
mod tests;
mod upload;
mod upload_control;
mod validators;
mod watch;
use args::*;
pub(super) use args::{Commands, RequestArgs, RequestCommand};
pub(super) fn check_request_file(path: &std::path::Path) -> Result<()> {
    request_files::check(path)
}
use focal_client::{Client, ClientError, RetryPolicy, UnixTransport, input::*, pending::*};
use focal_model::*;
use focal_node::{config::Settings, embedded::decode_identity};
use focal_wire::*;
pub(super) use mcp::serve;
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
    #[error(transparent)]
    Other(Box<dyn std::error::Error + Send + Sync>),
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
                let mut context = context::connect(profile, history)?;
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
        let identity =
            decode_identity(&root.join("IDENTITY")).map_err(|e| CliError::Other(Box::new(e)))?;
        // NETWORK also proves joined identity if its private JOIN files are
        // lost. Never fall back to the founder's issuer in that case.
        let actor =
            focal_node::network_join::local_unix_principal(&root, &identity, context::now()?)
                .map_err(|error| {
                    CliError::Input(format!("invalid local node client context: {error}"))
                })?;
        let limits = WireLimits::default();
        let transport = UnixTransport::connect(root.join("focal.sock"), limits.clone())
            .map_err(|e| CliError::Other(Box::new(e)))?;
        Ok(Self {
            client: Client::new(
                context::Transport::Unix(transport),
                RetryPolicy::default(),
                limits,
                1,
            )?,
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
fn random_id() -> std::result::Result<[u8; 16], InputError> {
    let mut bytes = [0; 16];
    getrandom::fill(&mut bytes).map_err(|_| InputError::Identity)?;
    if bytes == [0; 16] {
        return Err(InputError::Identity);
    }
    Ok(bytes)
}
pub(super) fn run(
    runtime: &tokio::runtime::Runtime,
    settings: &Settings,
    command: Commands,
    selection: Option<&str>,
) -> Result<()> {
    let context = Context::open(settings, selection)?;
    let command = match command {
        Commands::Claim {
            command: ClaimCommand::Wait(args),
        } => return claim_wait::run(runtime, &context, args),
        Commands::Monitor {
            command: monitor::MonitorCommand::Get(args),
        } => return monitor::get(runtime, &context, args),
        Commands::Watch { command } => return watch::run(runtime, &context, command),
        Commands::Ledger { command } => return graph::run(runtime, &context, command),
        Commands::Validator { command } => return validators::run(runtime, &context, command),
        Commands::Artifact {
            command: ArtifactCommand::Upload { command },
        } => return upload_control::run(runtime, &context, command),
        Commands::Get { command } => return reads::get(runtime, &context, command),
        Commands::List { command } => return reads::list(runtime, &context, command),
        Commands::Artifact {
            command: ArtifactCommand::Register(args),
        } if args
            .payload_file
            .as_ref()
            .is_some_and(|path| path != std::path::Path::new("-")) =>
        {
            return upload::register(runtime, &context, *args);
        }
        Commands::Submit {
            command: SubmitCommand::Artifact(args),
        } if args
            .payload_file
            .as_ref()
            .is_some_and(|path| path != std::path::Path::new("-")) =>
        {
            return upload::artifact(runtime, &context, args);
        }
        command => command,
    };
    let (authored, options) = authored::mutation(command)?;
    if options.operation.is_none() {
        return managed::submit(runtime, &context, authored, options);
    }
    let focal_client::operations::PlannedOperation::Mutation(command) =
        authored.build(&context.build, &mut random_id)?
    else {
        return Err(CliError::Input("mutation builder returned a read".into()));
    };
    submit(runtime, &context, command, options)
}
fn submit(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    command: Command,
    options: MutationOptions,
) -> Result<()> {
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
            runtime
                .block_on(context.client.claim_revision(
                    context.build.ledger,
                    claim,
                    request.request_id,
                ))?
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
    drive(runtime, context, &mut journal, options.output.format)
}
fn private_parent(path: &std::path::Path) -> Result<()> {
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
    Ok(())
}
fn drive(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    journal: &mut OperationJournal,
    format: OutputFormat,
) -> Result<()> {
    while let Some(request) = journal.next_request()?.cloned() {
        let reply = match runtime.block_on(context.client.submit(request)) {
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
pub(super) fn status(
    runtime: &tokio::runtime::Runtime,
    settings: &Settings,
    selection: Option<&str>,
) -> Result<()> {
    let context = Context::open(settings, selection)?;
    let request = context.envelope(Operation::Read(ReadRequest {
        consistency: ReadConsistency::Linearizable,
        query: ReadQuery::Objects(Vec::new()),
        max_items: 1,
    }))?;
    let reply = runtime.block_on(context.client.request(request))?;
    super::output_response(reply).map_err(CliError::Other)
}

pub(super) fn schema_validate(
    settings: &Settings,
    selection: Option<&str>,
    operation: &str,
    input: DocumentInput,
) -> Result<()> {
    let context = Context::open(settings, selection)?;
    discovery::validate(operation, input, Some(&context))
}
pub(super) fn request(
    runtime: &tokio::runtime::Runtime,
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
            request_files::send(runtime, settings, selection, &file)
        }
        (Some(file), None) => request_files::send(runtime, settings, selection, &file),
        (
            None,
            Some(RequestCommand::Retry {
                operation: Some(operation),
                operation_id: None,
                output,
            }),
        ) => {
            let context = Context::open(settings, selection)?;
            if upload::resume_legacy(runtime, &context, &operation, output.format)? {
                return Ok(());
            }
            let mut journal = OperationJournal::open(&operation, &context.operation)?;
            writeln!(
                std::io::stderr().lock(),
                "operation: {}",
                operation.display()
            )?;
            drive(runtime, &context, &mut journal, output.format)
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
                reconcile::inspect(runtime, &context, &journal, output.format)
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
        ) => managed::retry(
            runtime,
            &Context::open(settings, selection)?,
            &id,
            output.format,
        ),
        (
            None,
            Some(RequestCommand::Inspect {
                operation: None,
                operation_id: Some(id),
                remote,
                output,
            }),
        ) => managed::inspect(
            runtime,
            &Context::open(settings, selection)?,
            &id,
            remote,
            output.format,
        ),
        (None, Some(RequestCommand::Reserve { output })) => {
            managed::reserve(runtime, &Context::open(settings, selection)?, output.format)
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
        ) => managed::acknowledge(
            runtime,
            &Context::open(settings, selection)?,
            &operation_id,
            output.format,
        ),
        (
            None,
            Some(RequestCommand::Seal {
                operation_id,
                output,
            }),
        ) => managed::seal(
            runtime,
            &Context::open(settings, selection)?,
            &operation_id,
            output.format,
        ),
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
            reconcile::query(runtime, &context, query, output.format)
        }
        (None, Some(RequestCommand::Epoch { epoch, output })) => {
            let query = focal_client::operations::RequestEpochDocument { epoch }.build()?;
            let context = Context::open(settings, selection)?;
            reconcile::query(runtime, &context, query, output.format)
        }
        _ => Err(CliError::Input(
            "provide a wire request file or a request subcommand".into(),
        )),
    }
}
