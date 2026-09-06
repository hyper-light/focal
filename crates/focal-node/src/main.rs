#![cfg_attr(
    test,
    allow(
        clippy::panic,
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::unreachable,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros
    )
)]
use clap::{Parser, Subcommand};
use focal_model::*;
use focal_node::{
    config::Settings,
    embedded::{EmbeddedNode, decode_identity},
    host::LocalHost,
    placement::{self, NodeFacts},
};
use focal_wire::*;
use serde::Serialize;
use std::{
    collections::BTreeSet,
    fs::File,
    io::{Read, Write},
    path::{Path, PathBuf},
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Parser)]
#[command(
    name = "focal",
    version,
    about = "Durable claims, evidence, and validation ledger"
)]
struct Args {
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    #[arg(long, global = true)]
    data_dir: Option<PathBuf>,
    #[command(subcommand)]
    command: Commands,
}
#[derive(Subcommand)]
enum Commands {
    /// Run the durable local service. Local clients authenticate through OS credentials.
    Start,
    /// Run/resume the real claim/testament/validator example with exclusive local ownership.
    Demo,
    /// Read the running service's authoritative published prefix.
    Status,
    /// Send an explicit versioned request; retry the same file after an unknown outcome.
    Request { file: PathBuf },
    /// Show identity metadata without opening or modifying the ledger.
    Identity,
    /// Offline placement solver; planning does not activate a durability guarantee.
    Deployment {
        #[command(subcommand)]
        command: DeploymentCommand,
    },
}
#[derive(Subcommand)]
enum DeploymentCommand {
    /// Check the desired guarantee against an explicit inventory of verified node facts.
    Explain {
        #[arg(long)]
        inventory: Option<PathBuf>,
    },
    /// Emit the machine-readable deployment schema.
    Schema,
}

fn main() {
    if let Err(error) = execute() {
        let _ = writeln!(std::io::stderr().lock(), "focal: {error}");
        let mut source = error.source();
        while let Some(cause) = source {
            let _ = writeln!(std::io::stderr().lock(), "  caused by: {cause}");
            source = cause.source();
        }
        std::process::exit(1);
    }
}
fn execute() -> Result<()> {
    // Tokio's fallible builder can still unwind when an OS worker cannot be
    // started. Contain that dependency boundary before acquiring node state.
    let runtime = std::panic::catch_unwind(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
    })
    .map_err(|_| "async runtime initialization failed")??;
    let result = runtime.block_on(run());
    // A timed-out blocking owner must not make Runtime::drop wait forever.
    // Normal service shutdown has already joined its owner before returning.
    runtime.shutdown_background();
    result
}
async fn run() -> Result<()> {
    let args = Args::parse();
    let mut settings = match args.config {
        Some(path) => Settings::from_yaml(std::str::from_utf8(&read_file(&path, 64 * 1024)?)?)?,
        None => Settings::default(),
    };
    if let Some(data_dir) = args.data_dir {
        settings.node.data_dir = Some(data_dir);
    }
    settings.validate()?;
    match args.command {
        Commands::Start => start(settings).await,
        Commands::Demo => {
            let mut node = EmbeddedNode::open(&settings)?;
            let report = focal_node::demo::run(&mut node)?;
            node.checkpoint()?;
            print_json(&report)
        }
        Commands::Identity => print_json(&decode_identity(&settings.data_dir()?.join("IDENTITY"))?),
        Commands::Status => {
            let root = settings.data_dir()?;
            let identity = decode_identity(&root.join("IDENTITY"))?;
            let mut bytes = [0; 16];
            getrandom::fill(&mut bytes).map_err(|e| format!("randomness unavailable: {e}"))?;
            let request = RequestEnvelope {
                protocol: PROTOCOL_VERSION,
                ledger: identity.ledger,
                route_epoch: RouteEpoch(1),
                request_epoch: RequestEpoch(1),
                request_id: RequestId(bytes),
                operation: Operation::Read(ReadRequest {
                    consistency: ReadConsistency::Linearizable,
                    query: ReadQuery::Objects(vec![]),
                    max_items: 1,
                }),
            };
            let reply = UnixRemote::new(root.join("focal.sock"), WireLimits::default())?
                .request(&request)
                .await?;
            output_response(reply)
        }
        Commands::Request { file } => {
            let request: RequestEnvelope = serde_json::from_slice(&read_file(&file, 1024 * 1024)?)?;
            let reply = UnixRemote::new(
                settings.data_dir()?.join("focal.sock"),
                WireLimits::default(),
            )?
            .request(&request)
            .await?;
            output_response(reply)
        }
        Commands::Deployment {
            command: DeploymentCommand::Schema,
        } => {
            writeln!(
                std::io::stdout().lock(),
                "{}",
                include_str!("../../../config/schema/deployment-v1.json")
            )?;
            Ok(())
        }
        Commands::Deployment {
            command: DeploymentCommand::Explain { inventory },
        } => {
            let nodes: Vec<NodeFacts> = match inventory {
                Some(path) => serde_json::from_slice(&read_file(&path, 1024 * 1024)?)?,
                None => vec![NodeFacts {
                    id: 1,
                    topology: settings.topology.clone(),
                    verified: true,
                    eligible: true,
                }],
            };
            match placement::plan(&nodes, &settings.durability, &settings.placement) {
                Ok(plan) => print_json(
                    &serde_json::json!({"condition":"PlanValid","activated":false,"plan":plan}),
                ),
                Err(error) => {
                    print_json(
                        &serde_json::json!({"condition":"GuaranteeUnsatisfied","activated":false,"reason":error.to_string()}),
                    )?;
                    Err(error.into())
                }
            }
        }
    }
}
fn output_response(reply: ResponseEnvelope) -> Result<()> {
    let failed = match &reply.result {
        Response::Error(error) => Some(error.to_string()),
        Response::Submitted(MutationReply::Domain(DomainOutcome::Refuse { detail, .. })) => {
            Some(detail.clone())
        }
        Response::Submitted(MutationReply::Pending(_)) => {
            Some("outcome unknown; retry the same request file".into())
        }
        _ => None,
    };
    print_json(&reply)?;
    if let Some(error) = failed {
        Err(error.into())
    } else {
        Ok(())
    }
}
async fn start(settings: Settings) -> Result<()> {
    let node = EmbeddedNode::open(&settings)?;
    let path = node.root().join("focal.sock");
    // The exclusive data-directory owner may clean its socket after a crash, but
    // it must never remove an ordinary file or a symlink at that location.
    if let Ok(metadata) = std::fs::symlink_metadata(&path) {
        use std::os::unix::fs::{FileTypeExt, MetadataExt};
        if !metadata.file_type().is_socket()
            || metadata.uid() != std::fs::metadata(node.root())?.uid()
        {
            return Err("refusing to replace a non-owned socket path".into());
        }
        std::fs::remove_file(&path)?;
    }
    let grant = PeerGrant {
        principal: node.identity.issuer,
        tenants: BTreeSet::from([node.identity.ledger.tenant]),
        role: PeerRole::Runtime,
    };
    let limits = WireLimits::default();
    let server = UnixServer::bind(&path, grant, limits.clone())?;
    let identity = node.identity.clone();
    let (host, owner) = LocalHost::spawn(node, limits)?;
    let serving = server.serve(host.clone());
    tokio::pin!(serving);
    print_json(
        &serde_json::json!({"condition":"Ready","ledger":identity.ledger,"node":identity.node,"socket":path,"durability":{"survive":"node","max_failures":0},"meaning":"Acknowledged writes are synced to this disk; loss of this disk can lose the ledger."}),
    )?;
    // An ingress task failure is a shutdown trigger as well as a signal. Do
    // not advertise readiness forever after the listening service has failed.
    let mut finished = None;
    let signal_result = tokio::select! {
        result = shutdown_signal() => result,
        result = &mut serving => { finished = Some(result); Ok(()) }
        _ = host.closed() => Err(std::io::Error::other("node owner stopped")),
    };
    server.close();
    // Dropping a timed-out filesystem future would not stop its blocking owner.
    // Exiting the process after this bounded grace period gives the next owner
    // crash recovery, never an unsafe concurrent writer or a false clean exit.
    let cleanup = async move {
        let serving_result = match finished {
            Some(result) => result,
            None => serving.await,
        };
        let checkpoint_result = host.stop().await;
        owner.join_async().await?;
        serving_result?;
        checkpoint_result?;
        signal_result?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    };
    tokio::time::timeout(std::time::Duration::from_secs(30), cleanup)
        .await
        .map_err(|_| "shutdown deadline exceeded; recovery will replay the durable log")?
}
async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! { result=tokio::signal::ctrl_c()=>result, _=terminate.recv()=>Ok(()) }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await
}
fn read_file(path: &Path, max: usize) -> Result<Vec<u8>> {
    let file = File::open(path)?;
    if file.metadata()?.len() > max as u64 {
        return Err("input exceeds its byte budget".into());
    }
    let mut bytes = Vec::new();
    file.take(
        u64::try_from(max)?
            .checked_add(1)
            .ok_or("input byte budget overflow")?,
    )
    .read_to_end(&mut bytes)?;
    if bytes.len() > max {
        return Err("input exceeds its byte budget".into());
    }
    Ok(bytes)
}
fn print_json(value: &impl Serialize) -> Result<()> {
    writeln!(
        std::io::stdout().lock(),
        "{}",
        serde_json::to_string_pretty(value)?
    )?;
    Ok(())
}
