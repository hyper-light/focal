//! Existing transfer identities only: inspection never transmits; cancellation
//! persists its exact request before waiting for the authenticated content owner.
use super::{
    CliError, Context, Result,
    args::{OutputFormat, OutputOptions},
};
use clap::{Args, Subcommand, ValueEnum};
use focal_client::{
    ClientError,
    artifact_transfer::{TransferError, UploadJournal, UploadStore, UploadStoreLimits},
};
use focal_wire::{Operation, UploadRequest};
use std::{fs, io::Write, panic::AssertUnwindSafe, time::Duration};

#[derive(Subcommand)]
pub(crate) enum UploadCommand {
    /// Inspect saved transfer progress without sending or resuming any request.
    Inspect(UploadArgs),
    /// Retire a saved upload ID and remove server staging; retain committed content.
    Cancel(UploadArgs),
}
#[derive(Args)]
pub(crate) struct UploadArgs {
    /// Exact nonzero 32-character lowercase hexadecimal ID from upload progress.
    upload_id: String,
    /// Adapter history that owns this upload in the selected client context.
    #[arg(long, value_enum, default_value = "cli")]
    origin: Origin,
    #[command(flatten)]
    output: OutputOptions,
}
#[derive(Clone, Copy, ValueEnum)]
enum Origin {
    Cli,
    Mcp,
}
impl Origin {
    fn name(self) -> &'static str {
        match self {
            Self::Cli => "cli",
            Self::Mcp => "mcp",
        }
    }
    fn store(self) -> &'static str {
        match self {
            Self::Cli => "CLI.uploads",
            Self::Mcp => "MCP.uploads",
        }
    }
}
fn error(error: TransferError) -> CliError {
    CliError::Other(Box::new(error))
}
fn identifier(value: &str) -> Result<[u8; 16]> {
    if value.len() != 32
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(CliError::Input(
            "upload ID must be nonzero lowercase hexadecimal with 32 characters".into(),
        ));
    }
    let id =
        u128::from_str_radix(value, 16).map_err(|_| CliError::Input("invalid upload ID".into()))?;
    if id == 0 {
        return Err(CliError::Input("upload ID must be nonzero".into()));
    }
    Ok(id.to_be_bytes())
}
fn open(context: &Context, origin: Origin, id: [u8; 16]) -> Result<UploadJournal> {
    let name = origin.store();
    let mut present = false;
    for suffix in ["", ".lock", ".initialized"] {
        match fs::symlink_metadata(context.root.join(format!("{name}{suffix}"))) {
            Ok(_) => present = true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    if !present {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "no saved uploads for this adapter and client context",
        )
        .into());
    }
    // Prior bootstrap evidence selects open-only; missing/corrupt markers or a
    // lost catalogue fail closed rather than creating a new identity space.
    let store = UploadStore::bootstrap(
        &context.root,
        name,
        context.operation,
        UploadStoreLimits::default(),
    )
    .map_err(error)?;
    store.open_upload(id, &context.operation).map_err(error)
}
pub(super) fn run(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    command: UploadCommand,
) -> Result<()> {
    let (args, cancel) = match command {
        UploadCommand::Inspect(args) => (args, false),
        UploadCommand::Cancel(args) => (args, true),
    };
    let id = identifier(&args.upload_id)?;
    let mut journal = open(context, args.origin, id)?;
    let result = if cancel {
        cancel_upload(runtime, context, &mut journal)
    } else {
        Ok(())
    };
    let condition = if journal.cancel_requested() {
        if journal.progress().cancel_acknowledged {
            "CancelAcknowledged"
        } else {
            "CancelPending"
        }
    } else if result.is_err() {
        "OutcomeUnknown"
    } else if journal.reference().is_some() {
        "Sealed"
    } else {
        "Uploading"
    };
    let value = serde_json::json!({"schema_version":1,"upload_id":args.upload_id,"origin":args.origin.name(),"condition":condition,"cancel_requested":journal.cancel_requested(),"progress":journal.progress()});
    let output = (|| -> Result<()> {
        match args.output.format {
            OutputFormat::Json | OutputFormat::Yaml => {
                super::output::structured(&value, args.output.format)?
            }
            OutputFormat::Table => {
                let mut out = std::io::stdout().lock();
                let progress = journal.progress();
                writeln!(
                    out,
                    "UPLOAD\t{}\nORIGIN\t{}\nCONDITION\t{}\nLENGTH\t{}\nSTAGED\t{}\nRECEIVED\t{}\nCANCEL REQUESTED\t{}\nCANCEL ACKNOWLEDGED\t{}\nSEALED CONTENT\t{}",
                    args.upload_id,
                    args.origin.name(),
                    condition,
                    progress.length,
                    progress.staged,
                    progress.received,
                    journal.cancel_requested(),
                    progress.cancel_acknowledged,
                    progress.reference.is_some()
                )?;
                out.flush()?;
            }
        }
        Ok(())
    })();
    if result.is_err() {
        let invocation = context.invocation.as_deref().unwrap_or("focal");
        let _ = writeln!(
            std::io::stderr().lock(),
            "Transfer cancellation remains saved. Recovery: {invocation} artifact upload cancel {} --origin {}",
            args.upload_id,
            args.origin.name()
        );
    }
    // A failed sink does not make an unresolved cancellation into an ordinary
    // output error. Keep the original failure and exact recovery identity.
    result.and(output)
}
fn cancel_upload(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    journal: &mut UploadJournal,
) -> Result<()> {
    journal.cancel().map_err(error)?;
    let request = journal
        .next_request()
        .map_err(error)?
        .ok_or(CliError::InvalidResponse)?;
    if !matches!(request.operation,Operation::Upload(UploadRequest::Cancel{upload}) if upload==journal.progress().upload)
    {
        return Err(CliError::InvalidResponse);
    }
    let reply = std::panic::catch_unwind(AssertUnwindSafe(|| {
        runtime.block_on(async {
            tokio::time::timeout(
                Duration::from_secs(30),
                context.client.upload(request.clone()),
            )
            .await
            .map_err(|_| ClientError::OutcomeUnknown {
                request: Box::new(request.clone()),
            })?
        })
    }))
    .map_err(|_| ClientError::OutcomeUnknown {
        request: Box::new(request.clone()),
    })??;
    journal.record_reply(&request, &reply).map_err(error)
}
