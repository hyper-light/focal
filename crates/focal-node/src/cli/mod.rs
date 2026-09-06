mod args;
mod authored;
mod documents;
mod download;
mod mcp;
mod output;
mod reads;
mod reconcile;
#[cfg(test)]
mod tests;
use args::*;
pub(super) use args::{Commands, RequestArgs};
use focal_client::{Client, ClientError, RetryPolicy, UnixTransport, input::*, pending::*};
use focal_model::*;
use focal_node::{config::Settings, embedded::decode_identity};
use focal_wire::*;
pub(super) use mcp::serve;
use std::{io::Write, path::PathBuf};

type Result<T> = std::result::Result<T, CliError>;
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
    Io(#[from] std::io::Error),
    #[error("object not found at the observed ledger prefix")]
    NotFound,
    #[error("more than one claim matches; use list claims or an exact ID")]
    Ambiguous,
    #[error("invalid or inconsistent server response")]
    InvalidResponse,
    #[error("mutation did not commit: {0:?}; its exact request remains journaled")]
    Domain(DomainOutcome),
    #[error("mutation has no verified durable receipt; retry the saved operation")]
    Unconfirmed,
    #[error(transparent)]
    Other(Box<dyn std::error::Error + Send + Sync>),
}
impl CliError {
    pub(super) fn exit_code(&self) -> i32 {
        match self {
            Self::Input(_) | Self::Document(_) => 2,
            Self::Client(ClientError::Access(AccessError::Unauthorized)) => 3,
            Self::NotFound => 4,
            Self::Ambiguous | Self::Domain(_) => 5,
            Self::Pending(PendingError::Locked) => 6,
            Self::Client(ClientError::OutcomeUnknown { .. }) | Self::Unconfirmed => 7,
            _ => 1,
        }
    }
}
pub(super) struct Context {
    client: Client<UnixTransport>,
    build: BuildContext,
    operation: OperationContext,
    root: PathBuf,
}
impl Context {
    fn open(settings: &Settings) -> Result<Self> {
        let root = settings
            .data_dir()
            .map_err(|e| CliError::Other(Box::new(e)))?;
        // Joined Unix ingress authenticates the enrolled local principal, while
        // IDENTITY retains the founder's issuer. Until an authenticated client
        // context is exposed, never journal or query under that wrong identity.
        for marker in ["JOIN", "JOIN.initialized"] {
            match std::fs::symlink_metadata(root.join(marker)) {
                Ok(_) => return Err(CliError::Input(
                    "manual client context on a joined node is unsupported until authenticated client contexts are available".into(),
                )),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        let identity =
            decode_identity(&root.join("IDENTITY")).map_err(|e| CliError::Other(Box::new(e)))?;
        let limits = WireLimits::default();
        let transport = UnixTransport::connect(root.join("focal.sock"), limits.clone())
            .map_err(|e| CliError::Other(Box::new(e)))?;
        Ok(Self {
            client: Client::new(transport, RetryPolicy::default(), limits, 1)?,
            build: BuildContext {
                ledger: identity.ledger,
                actor: identity.issuer,
                root: identity.root,
                policy_revision: 1,
            },
            operation: OperationContext {
                cluster: identity.cluster,
                principal: identity.issuer,
                ledger: identity.ledger,
            },
            root,
        })
    }
    fn envelope(&self, operation: Operation) -> Result<RequestEnvelope> {
        Ok(RequestEnvelope {
            protocol: PROTOCOL_VERSION,
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
) -> Result<()> {
    let context = Context::open(settings)?;
    let command = match command {
        Commands::Get { command } => return reads::get(runtime, &context, command),
        Commands::List { command } => return reads::list(runtime, &context, command),
        command => command,
    };
    let (authored, options) = authored::mutation(command)?;
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
    let request = context.envelope(Operation::Submit {
        expected_revision: options.expected_revision.map(ObjectRevision),
        command,
    })?;
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
pub(super) fn request(
    runtime: &tokio::runtime::Runtime,
    settings: &Settings,
    args: RequestArgs,
) -> Result<()> {
    match (args.file, args.command) {
        (Some(file), None) => {
            let request: RequestEnvelope =
                serde_json::from_slice(&documents::read_bytes(&file, 1024 * 1024)?)
                    .map_err(|e| CliError::Input(e.to_string()))?;
            let root = settings
                .data_dir()
                .map_err(|e| CliError::Other(Box::new(e)))?;
            let remote = UnixRemote::new(root.join("focal.sock"), WireLimits::default())
                .map_err(|e| CliError::Other(Box::new(e)))?;
            let response = runtime
                .block_on(remote.request(&request))
                .map_err(|e| CliError::Other(Box::new(e)))?;
            super::output_response(response).map_err(CliError::Other)
        }
        (None, Some(RequestCommand::Retry { operation, output })) => {
            let context = Context::open(settings)?;
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
                remote,
                output,
            }),
        ) => {
            let context = Context::open(settings)?;
            let journal = OperationJournal::open(operation, &context.operation)?;
            if remote {
                reconcile::inspect(runtime, &context, &journal, output.format)
            } else {
                output::journal(&journal, output.format)
            }
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
            let context = Context::open(settings)?;
            reconcile::query(runtime, &context, query, output.format)
        }
        (None, Some(RequestCommand::Epoch { epoch, output })) => {
            let query = focal_client::operations::RequestEpochDocument { epoch }.build()?;
            let context = Context::open(settings)?;
            reconcile::query(runtime, &context, query, output.format)
        }
        _ => Err(CliError::Input(
            "provide a wire request file or a request subcommand".into(),
        )),
    }
}
