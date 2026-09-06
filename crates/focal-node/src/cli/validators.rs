use super::{
    Context, Result,
    args::{OutputFormat, OutputOptions},
};
use clap::{Args, Subcommand};
use focal_client::operations::ValidatorDocument;
use focal_wire::{Operation, ReadObject};
use std::io::Write;

#[derive(Subcommand)]
pub(crate) enum ValidatorCommand {
    /// List recorded contracts. Every filter is optional; handlers execute externally.
    List {
        #[arg(long)]
        id: Option<String>,
        #[arg(long, requires = "id")]
        version: Option<String>,
        #[command(flatten)]
        filters: ContractFilters,
    },
    /// Inspect requirement bindings of one exact immutable handler version.
    Get {
        id: String,
        #[arg(long)]
        version: String,
        #[command(flatten)]
        filters: ContractFilters,
    },
}
#[derive(Args)]
pub(crate) struct ContractFilters {
    #[arg(long)]
    claim: Option<String>,
    #[arg(long)]
    evaluator: Option<String>,
    #[arg(long)]
    kind: Option<String>,
    #[arg(long)]
    phase: Option<String>,
    #[arg(long)]
    mode: Option<String>,
    /// true selects agentic handlers; false selects programmatic handlers.
    #[arg(long, num_args = 1)]
    agentic: Option<bool>,
    #[arg(long)]
    schema_hash: Option<String>,
    #[arg(long)]
    cursor: Option<String>,
    #[arg(long, default_value_t = 64)]
    limit: u32,
    #[arg(long, default_value_t = 1024)]
    max_visits: u32,
    #[command(flatten)]
    output: OutputOptions,
}
pub(super) fn run(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    command: ValidatorCommand,
) -> Result<()> {
    let (id, version, filters, exact) = match command {
        ValidatorCommand::List {
            id,
            version,
            filters,
        } => (id, version, filters, false),
        ValidatorCommand::Get {
            id,
            version,
            filters,
        } => (Some(id), Some(version), filters, true),
    };
    let query = ValidatorDocument {
        id,
        version,
        claim: filters.claim,
        evaluator: filters.evaluator,
        kind: filters.kind,
        phase: filters.phase,
        mode: filters.mode,
        agentic: filters.agentic,
        schema_hash: filters.schema_hash,
        cursor: filters.cursor,
        limit: filters.limit,
        max_visits: filters.max_visits,
    }
    .build(&context.build, exact)?;
    let page = runtime.block_on(
        context
            .client
            .validators(context.envelope(Operation::Validators(query.clone()))?),
    )?;
    match filters.output.format {
        OutputFormat::Json | OutputFormat::Yaml => {
            #[derive(serde::Serialize)]
            struct ResultPage<'a> {
                schema_version: u16,
                execution: &'static str,
                page: &'a focal_wire::ListPage,
                cursor: Option<String>,
            }
            super::output::structured(
                &ResultPage {
                    schema_version: 1,
                    execution: "participant_owned",
                    cursor: page
                        .next
                        .as_ref()
                        .map(|cursor| super::output::hex(&cursor.bytes)),
                    page: &page,
                },
                filters.output.format,
            )
        }
        OutputFormat::Table => {
            let mut out = std::io::stdout().lock();
            writeln!(out, "VALIDATOR\tVERSION\tKIND\tREQUIREMENT\tEVALUATOR")?;
            for object in &page.objects {
                let ReadObject::Validation { id, value } = object else {
                    return Err(super::CliError::InvalidResponse);
                };
                for handler in value
                    .content()
                    .handlers
                    .iter()
                    .filter(|handler| query.matches_handler(handler))
                {
                    writeln!(
                        out,
                        "{}\t{}\t{}\t{}\t{}",
                        handler.id,
                        handler.version,
                        if handler.agentic {
                            "agentic"
                        } else {
                            "programmatic"
                        },
                        id,
                        value.content().evaluator
                    )?;
                }
            }
            writeln!(out, "SEQUENCE\t{}", page.token.sequence.0)?;
            writeln!(out, "EXECUTION\tparticipant owned")?;
            if let Some(cursor) = page.next {
                writeln!(out, "CURSOR\t{}", super::output::hex(&cursor.bytes))?;
            }
            Ok(())
        }
    }
}
