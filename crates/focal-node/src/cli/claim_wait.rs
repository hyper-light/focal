use super::{
    CliError, Context, Result,
    args::{DocumentInput, OutputFormat},
    output,
};
use clap::{Args, ValueEnum};
use focal_client::{
    claim_wait::*,
    operations::{ApplicationResult, ClaimWaitDocument, OperationOutput},
};
use focal_wire::Operation;
use std::io::Write;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum Until {
    Satisfied,
    Terminal,
    Released,
}
impl From<Until> for ClaimWaitUntil {
    fn from(value: Until) -> Self {
        match value {
            Until::Satisfied => Self::Satisfied,
            Until::Terminal => Self::Terminal,
            Until::Released => Self::Released,
        }
    }
}
#[derive(Args)]
pub(crate) struct WaitArgs {
    #[command(flatten)]
    input: DocumentInput,
    #[arg(required_unless_present_any = ["json", "yaml", "file"])]
    claim: Option<String>,
    #[arg(long, value_enum, required_unless_present_any = ["json", "yaml", "file"])]
    until: Option<Until>,
    /// Observer deadline in milliseconds, 1..30000; use watch for longer waits.
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..=30000))]
    timeout_ms: Option<u32>,
    #[command(flatten)]
    output: super::args::OutputOptions,
}
pub(super) fn run(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    args: WaitArgs,
) -> Result<()> {
    let fields = args.claim.is_some() || args.until.is_some() || args.timeout_ms.is_some();
    let document: ClaimWaitDocument = match args.input.load(fields)? {
        Some(document) => document,
        None => ClaimWaitDocument {
            claim: args
                .claim
                .ok_or_else(|| CliError::Input("claim is required".into()))?,
            until: args
                .until
                .ok_or_else(|| CliError::Input("--until is required".into()))?
                .into(),
            timeout_ms: args.timeout_ms.unwrap_or(30_000),
        },
    };
    let (read, until, timeout_ms) = document.build(&context.build)?;
    let request = context.envelope(Operation::Read(read))?;
    let result = runtime.block_on(async {tokio::select! {
        result=context.client.claim_wait(request,until,std::time::Duration::from_millis(u64::from(timeout_ms)))=>result.map_err(map_error),
        signal=tokio::signal::ctrl_c()=>{ signal?; Err(CliError::Io(std::io::Error::new(std::io::ErrorKind::Interrupted,"claim observation interrupted"))) },
    }})?;
    let mut stdout = std::io::stdout().lock();
    match args.output.format {
        OutputFormat::Table => writeln!(
            stdout,
            "{} {} {:?} at sequence {} (released: {})",
            result.condition.as_str(),
            result.observation.id,
            result.observation.status,
            result.observation.token.sequence.0,
            result.observation.released
        )?,
        format => output::structured_to(
            &mut stdout,
            &ApplicationResult {
                schema_version: 1,
                operation_id: None,
                condition: result.condition.as_str().into(),
                result: OperationOutput::ClaimWait { result },
            },
            format,
        )?,
    }
    stdout.flush()?;
    if result.condition != ClaimWaitCondition::Met {
        return Err(CliError::WaitUnfinished(result.condition));
    }
    Ok(())
}
fn map_error(error: ClaimWaitError) -> CliError {
    match error {
        ClaimWaitError::Client(error) => CliError::Client(error),
        ClaimWaitError::NotFound => CliError::NotFound,
        ClaimWaitError::InvalidRequest => CliError::Input("invalid claim wait query".into()),
    }
}
