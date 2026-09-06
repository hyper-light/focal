use super::*;
use clap::{Args, Subcommand};
use focal_client::watch::{
    WatchAction, WatchDelivery, WatchJournal, WatchOptions, WatchPage, WatchStore,
};
use std::{io, time::Duration};
use tokio::io::AsyncWriteExt;
#[derive(Subcommand)]
pub(crate) enum WatchCommand {
    /// Watch claim facts. No --claim filter means all claims.
    Claims(Open),
    Testaments(Open),
    Artifacts(Open),
    Validations(Open),
    All(Open),
    /// Resume saved options and any page interrupted before output was flushed.
    Resume(Resume),
    /// Inspect saved names, or one named watch and retained page, without ACK.
    Inspect(Inspect),
}
#[derive(Args)]
pub(crate) struct Open {
    /// Durable local watch name; separate names have independent cursors.
    #[arg(long)]
    name: Option<String>,
    #[arg(long = "claim")]
    claims: Vec<String>,
    /// Begin at the retained tail without an initial snapshot seed.
    #[arg(long)]
    no_seed: bool,
    #[arg(long,default_value_t=64,value_parser=clap::value_parser!(u32).range(1..=256))]
    limit: u32,
    #[command(flatten)]
    follow: Follow,
}
#[derive(Args)]
pub(crate) struct Resume {
    name: String,
    #[command(flatten)]
    follow: Follow,
}
#[derive(Args)]
pub(crate) struct Inspect {
    name: Option<String>,
    #[command(flatten)]
    output: OutputOptions,
}
#[derive(Args)]
struct Follow {
    /// Stop after this many flushed pages; omit to follow until Ctrl-C.
    #[arg(long,value_parser=clap::value_parser!(u64).range(1..))]
    pages: Option<u64>,
    #[command(flatten)]
    output: OutputOptions,
}
pub(super) fn run(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    command: WatchCommand,
) -> Result<()> {
    let store = WatchStore::open(&context.root, context.operation).map_err(other)?;
    let (mut journal, follow) = match command {
        WatchCommand::Inspect(args) => {
            let value = if let Some(name) = args.name {
                let journal = store.resume(&name).map_err(other)?;
                focal_client::operations::OperationOutput::Watch {
                    status: journal.status(),
                    delivery: journal.delivery().cloned().map(Box::new),
                }
            } else {
                focal_client::operations::OperationOutput::Watches {
                    names: store.names().map_err(other)?,
                }
            };
            let mut out = io::stdout().lock();
            if matches!(args.output.format, OutputFormat::Table) {
                match &value {
                    focal_client::operations::OperationOutput::Watches { names } => {
                        writeln!(out, "WATCH")?;
                        for name in names {
                            writeln!(out, "{name}")?;
                        }
                    }
                    focal_client::operations::OperationOutput::Watch { status, delivery } => {
                        writeln!(out, "WATCH\tDELIVERED\tCONSUMED\tPENDING")?;
                        writeln!(
                            out,
                            "{}\t{}\t{}\t{}",
                            status.name, status.delivered, status.acknowledged, status.pending
                        )?;
                        if let Some(delivery) = delivery {
                            writeln!(out, "RETAINED\t{}", delivery.id)?;
                        }
                    }
                    _ => return Err(CliError::InvalidResponse),
                }
            } else {
                super::output::structured_to(&mut out, &value, args.output.format)?;
            }
            out.flush()?;
            return Ok(());
        }
        WatchCommand::Resume(args) => (store.resume(&args.name).map_err(other)?, args.follow),
        command => {
            let (family, args) = match command {
                WatchCommand::Claims(args) => (Some(ObjectKind::Claim), args),
                WatchCommand::Artifacts(args) => (Some(ObjectKind::Artifact), args),
                WatchCommand::Testaments(args) => (Some(ObjectKind::Testament), args),
                WatchCommand::Validations(args) => (Some(ObjectKind::Validation), args),
                WatchCommand::All(args) => (None, args),
                _ => return Err(CliError::InvalidResponse),
            };
            let mut claims = args
                .claims
                .iter()
                .map(|text| parse_id(text).map(ClaimId))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            claims.sort_unstable();
            claims.dedup();
            let options = WatchOptions {
                claims,
                family,
                seed: !args.no_seed,
                max_items: args.limit,
                max_bytes: 65536,
            };
            let name = args.name.unwrap_or_else(|| default_name(&options));
            (store.create(&name, options).map_err(other)?, args.follow)
        }
    };
    let name = journal.status().name;
    let result = follow_pages(runtime, context, &mut journal, follow);
    match result {
        Ok(true) => {
            recovery(context, &name);
            Ok(())
        }
        Ok(false) => Ok(()),
        Err(error) => {
            recovery(context, &name);
            Err(error)
        }
    }
}
fn follow_pages(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    journal: &mut WatchJournal,
    follow: Follow,
) -> Result<bool> {
    let mut count = 0u64;
    let mut stdout = tokio::io::stdout();
    let mut interrupted = std::pin::pin!(tokio::signal::ctrl_c());
    loop {
        match journal.next_action(&mut random_id).map_err(other)? {
            WatchAction::Request(action) => {
                let reply=runtime.block_on(async{tokio::select!{result=context.client.request(action.request.clone())=>result.map(Some).map_err(CliError::Client),signal=&mut interrupted=>{signal?;Ok(None)}}})?;
                let Some(reply) = reply else {
                    return Ok(true);
                };
                journal.accept(action, reply).map_err(other)?;
            }
            WatchAction::Delivery => {
                let delivery = journal.delivery().ok_or(CliError::InvalidResponse)?;
                let id = delivery.id;
                let bytes = encode_delivery(delivery, follow.output.format)?;
                let flushed=runtime.block_on(async{tokio::select!{result=async{stdout.write_all(&bytes).await?;stdout.flush().await}=>{result?;Ok::<_,io::Error>(true)},signal=&mut interrupted=>{signal?;Ok(false)}}})?;
                if !flushed {
                    return Ok(true);
                }
                journal.acknowledge(id).map_err(other)?;
                count = count
                    .checked_add(1)
                    .ok_or_else(|| CliError::Input("watch page counter exhausted".into()))?;
                if follow.pages.is_some_and(|limit| count >= limit) {
                    return Ok(false);
                }
                let stopped=runtime.block_on(async{tokio::select!{_=tokio::time::sleep(Duration::from_millis(250))=>Ok::<_,io::Error>(false),signal=&mut interrupted=>{signal?;Ok(true)}}})?;
                if stopped {
                    return Ok(true);
                }
            }
        }
    }
}
fn other(error: impl std::error::Error + Send + Sync + 'static) -> CliError {
    CliError::Other(Box::new(error))
}
fn default_name(options: &WatchOptions) -> String {
    let family = match options.family {
        Some(ObjectKind::Claim) => "claims",
        Some(ObjectKind::Testament) => "testaments",
        Some(ObjectKind::Artifact) => "artifacts",
        Some(ObjectKind::Validation) => "validations",
        None => "all",
    };
    if options.claims.is_empty() {
        return family.into();
    }
    let mut hash = blake3::Hasher::new();
    for id in &options.claims {
        hash.update(&id.0);
    }
    let suffix: String = hash.finalize().to_hex().chars().take(16).collect();
    format!("{family}-{suffix}")
}
fn recovery(context: &Context, name: &str) {
    let mut err = io::stderr().lock();
    if let Some(invocation) = context.invocation.as_deref() {
        let _ = writeln!(
            err,
            "Resume retained watch: {invocation} watch resume {}",
            quote(name)
        );
    } else {
        let _ = writeln!(
            err,
            "Resume retained watch: focal watch resume {} (keep the original --data-dir/--client-context)",
            quote(name)
        );
    }
}
fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}
struct Output(Vec<u8>);
impl io::Write for Output {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let size = self
            .0
            .len()
            .checked_add(bytes.len())
            .filter(|size| *size <= 1024 * 1024)
            .ok_or_else(|| io::Error::other("watch output exceeds one MiB"))?;
        if size > self.0.capacity() {
            self.0
                .try_reserve(size.saturating_sub(self.0.len()))
                .map_err(|_| io::Error::other("watch output capacity"))?;
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn encode_delivery(delivery: &WatchDelivery, format: OutputFormat) -> Result<Vec<u8>> {
    let mut out = Output(Vec::new());
    if matches!(format, OutputFormat::Yaml) {
        writeln!(out, "---")?;
        super::output::structured_to(&mut out, delivery, format)?;
        return Ok(out.0);
    }
    if matches!(format, OutputFormat::Json) {
        serde_json::to_writer(&mut out, delivery).map_err(other)?;
        writeln!(out)?;
        return Ok(out.0);
    }
    writeln!(
        out,
        "DELIVERY\t{}\t{}",
        delivery.number,
        blake3::Hash::from_bytes(delivery.id.0).to_hex()
    )?;
    match &delivery.page {
        WatchPage::Seed { page } => {
            for object in &page.objects {
                let (kind, id) = match object {
                    ReadObject::Claim { id, .. } => ("Claim", id.0),
                    ReadObject::Testament { id, .. } => ("Testament", id.0),
                    ReadObject::Artifact { id, .. } => ("Artifact", id.0),
                    ReadObject::Validation { id, .. }
                    | ReadObject::ValidationResults { id, .. } => ("Validation", id.0),
                };
                writeln!(out, "SEED\t{kind}\t{}", ObjectId(id))?;
            }
            writeln!(out, "PREFIX\t{}", page.token.sequence.0)?;
        }
        WatchPage::Events { page } => {
            for event in &page.events {
                match event {
                    StreamEvent::Delta { delta, .. } => {
                        writeln!(out, "CHANGE\t{}\t{:?}", delta.id.sequence.0, delta.fact)?
                    }
                    StreamEvent::Resolved { cursor } => {
                        writeln!(out, "RESOLVED\t{}", cursor.position.sequence.0)?
                    }
                    StreamEvent::Resync { reason, floor, .. } => {
                        writeln!(out, "RESYNC\t{reason:?}\t{}", floor.0)?
                    }
                }
            }
        }
    }
    Ok(out.0)
}
