use super::{Context, Result, args::OutputOptions};
use clap::{Args, Subcommand};
use focal_client::operations::TraversalDocument;

#[derive(Subcommand)]
pub(crate) enum LedgerCommand {
    /// Read bounded scalar counts at a fresh quorum prefix of this ledger.
    Summary(OutputOptions),
    /// Follow indexed graph edges at one fixed prefix. Truncation is explicit.
    Traverse(TraverseArgs),
}
#[derive(Args)]
pub(crate) struct TraverseArgs {
    /// One or more typed roots, such as claim:00000000000000000000000000000001.
    #[arg(required=true,num_args=1..)]
    roots: Vec<String>,
    #[arg(long,default_value="forward",value_parser=["forward","reverse"])]
    direction: String,
    /// Canonical relation or family edge; repeated options form a union.
    #[arg(long = "edge")]
    edges: Vec<String>,
    #[arg(long, default_value_t = 8)]
    depth: u16,
    #[arg(long, default_value_t = 4096)]
    max_nodes: u32,
    #[arg(long, default_value_t = 16_384)]
    max_edges: u32,
    #[arg(long, default_value_t = 64)]
    limit: u32,
    #[arg(long, default_value_t = 1024)]
    max_visits: u32,
    #[arg(long, default_value_t = 1_048_576)]
    max_bytes: u32,
    /// Opaque cursor from the preceding page; preserve all other query options.
    #[arg(long)]
    cursor: Option<String>,
    #[command(flatten)]
    output: OutputOptions,
}
pub(super) fn run(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    command: LedgerCommand,
) -> Result<()> {
    match command {
        LedgerCommand::Summary(output) => {
            let summary = runtime.block_on(
                context
                    .client
                    .ledger_summary(context.envelope(focal_wire::Operation::Summary)?),
            )?;
            match output.format {
                super::args::OutputFormat::Json | super::args::OutputFormat::Yaml => {
                    super::output::structured(
                        &serde_json::json!({"schema_version":1,"summary":summary}),
                        output.format,
                    )
                }
                super::args::OutputFormat::Table => {
                    use std::io::Write;
                    let mut out = std::io::stdout().lock();
                    writeln!(
                        out,
                        "LEDGER\t{}/{}\nSEQUENCE\t{}\nROUTE EPOCH\t{}\nAPPLIED INDEX\t{}\nCLAIMS\t{}\nTESTAMENTS\t{}\nARTIFACTS\t{}\nVALIDATIONS\t{}\nEVIDENCE SETS\t{}\nVALIDATION RUNS\t{}",
                        summary.token.ledger.tenant,
                        summary.token.ledger.session,
                        summary.token.sequence.0,
                        summary.token.route_epoch.0,
                        summary.applied_index,
                        summary.claims,
                        summary.testaments,
                        summary.artifacts,
                        summary.validations,
                        summary.evidence_sets,
                        summary.validation_runs
                    )?;
                    Ok(())
                }
            }
        }
        LedgerCommand::Traverse(args) => {
            let query = TraversalDocument {
                roots: args.roots,
                direction: args.direction,
                edges: args.edges,
                depth: args.depth,
                max_nodes: args.max_nodes,
                max_edges: args.max_edges,
                limit: args.limit,
                max_visits: args.max_visits,
                max_bytes: args.max_bytes,
                cursor: args.cursor,
            }
            .build(&context.build)?;
            let request = context.envelope(focal_wire::Operation::Traverse(query))?;
            let page = runtime.block_on(context.client.traverse(request))?;
            super::output::traversal(page, args.output.format)
        }
    }
}
