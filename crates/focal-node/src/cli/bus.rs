//! `focal start bus`: one node of the event bus (doc 32), in its standalone mode.
//!
//! The node admits the client identities its local directory holds (`--pki`, made at the first
//! start: an authority, each node's certificate and `--clients` client identities, as DER files a
//! client reads), keeps what its memory holds of the batches for subscribers that start behind, and
//! serves until an interrupt or a termination. It prints one JSON line when it listens and one when
//! it stops, with what it did.
//!
//! Alone, it holds the ephemeral class. With `--node` and a `--peer` for each other node, it is a
//! node of a part's group (doc 34): the part replicated by the nodes, its log sealed in `--data`
//! under the root key in `--key-file`, its publishes acknowledged once a majority's disks hold them.

use clap::{Args, Subcommand};
use std::net::SocketAddr;
use std::path::PathBuf;

/// The event bus's commands.
#[derive(Subcommand)]
pub(crate) enum BusCommand {
    /// Run one event bus node until it is interrupted or terminated.
    Start(BusStart),
}

/// `focal start bus`'s arguments.
#[derive(Args)]
pub(crate) struct BusStart {
    /// The address the node listens on (UDP, QUIC).
    #[arg(long)]
    pub(crate) listen: SocketAddr,
    /// The directory of the node's local identities: made at the first start, read after.
    #[arg(long)]
    pub(crate) pki: PathBuf,
    /// Client identities the directory holds and the node admits: at least one, at most what the
    /// node's memory admits.
    #[arg(long, default_value_t = 2)]
    pub(crate) clients: usize,
    /// This node of a part's group (from 1), as the local directory names it: with `--peer`, the
    /// node replicates its part with the others (doc 34).
    #[arg(long, requires = "peer", requires = "data")]
    pub(crate) node: Option<u64>,
    /// Another node of the group, as `ID=ADDRESS`: one for each.
    #[arg(long, value_parser = parse_peer, requires = "node")]
    pub(crate) peer: Vec<(u64, SocketAddr)>,
    /// The directory a node of a group keeps its log, its part's image and its run in.
    #[arg(long, requires = "node")]
    pub(crate) data: Option<PathBuf>,
    /// The root key file a node of a group keeps its log's keys under, outside `--data`; by default
    /// the platform's per-user key directory's `focal-bus-node-ID.key`.
    #[arg(long, requires = "node")]
    pub(crate) key_file: Option<PathBuf>,
}

/// A `--peer` value: `ID=ADDRESS`.
fn parse_peer(value: &str) -> Result<(u64, SocketAddr), String> {
    let (id, address) = value
        .split_once('=')
        .ok_or_else(|| format!("a peer is ID=ADDRESS, not {value:?}"))?;
    let id = id
        .parse::<u64>()
        .map_err(|_| format!("a peer's id is a number, not {id:?}"))?;
    let address = address
        .parse::<SocketAddr>()
        .map_err(|_| format!("a peer's address is HOST:PORT, not {address:?}"))?;
    Ok((id, address))
}

/// The bytes the node holds: the machine's envelope for a process (`focal_platform::memory_envelope`),
/// as a ledger node's is. Nothing to set: the node is sized by the machine it finds, and refuses to
/// start on one whose memory it cannot read rather than guess at it.
fn node_bytes() -> Result<usize, focal_bus::BusError> {
    let bytes = focal_platform::memory_envelope().ok_or(focal_bus::BusError::Configuration(
        "the machine's memory could not be read",
    ))?;
    Ok(usize::try_from(bytes).unwrap_or(usize::MAX))
}

/// Runs the node on one shard of this thread ([`focal_rt::local_config`]) until it is told to
/// stop.
pub(crate) fn run(args: BusStart) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut runtime = hyper_rt::runtime::LocalRuntime::new(&focal_rt::local_config())?;
    runtime.block_on(start(args))??;
    Ok(())
}

/// Runs the node.
pub(crate) async fn start(args: BusStart) -> Result<(), focal_bus::BusError> {
    let memory_bytes = node_bytes()?;
    let group = match (args.node, args.data) {
        (Some(node), Some(data)) => {
            let key_file = match args.key_file {
                Some(path) => path,
                None => focal_seal::default_key_file(&focal_bus::pki::node_name(node)).ok_or(
                    focal_bus::BusError::Configuration("no key file named, and no default for it"),
                )?,
            };
            Some(focal_bus::serve::GroupServe {
                node,
                peers: args.peer,
                data,
                key_file,
            })
        }
        _ => None,
    };
    let config = focal_bus::serve::ServeConfig {
        listen: args.listen,
        pki: args.pki,
        clients: args.clients,
        node: focal_bus::node::NodeConfig::sized(memory_bytes),
        memory_bytes,
        group,
    };
    let (stats, memory, buffers, rooms, counters) = focal_bus::serve::serve(config, |address| {
        let line = serde_json::json!({ "condition": "Listening", "listen": address.to_string() });
        let _ = std::io::Write::write_all(
            &mut std::io::stdout().lock(),
            format!("{line}\n").as_bytes(),
        );
    })
    .await?;
    let line = serde_json::json!({
        "condition": "Stopped",
        "published": stats.published,
        "refused": stats.refused,
        "delivered": stats.delivered,
        "gaps": stats.gaps,
        "blocked": stats.blocked,
        "subscribed": stats.subscribed,
        "held": stats.held,
        "starved": stats.starved,
        "dropped": stats.dropped,
        // Memory taken from and returned to the system: a segment or a buffer allocated anew is
        // faulted in page by page, one kept warm is not.
        "segments": {
            "allocated": stats.segments_allocated,
            "released": stats.segments_released,
        },
        // What the node's memory held as it stopped: the window's segments, the transport's
        // buffers and socket, the tables.
        "memory": {
            "limit": memory.limit,
            "used": memory.used,
            "segments": memory.used_by(focal_memory::BudgetKind::Payload),
            "transport": memory.used_by(focal_memory::BudgetKind::Pending),
            "tables": memory.used_by(focal_memory::BudgetKind::Index),
        },
        "buffers": {
            "allocated": buffers.allocated,
            "released": buffers.released,
            "refused": buffers.refused,
        },
        "rooms": {
            "least": rooms.least,
            "most": rooms.most,
            "measured": rooms.measured,
            "unmeasured": rooms.unmeasured,
        },
        // The shard's loop: what each message cost it in steps, waits and the reads its learning
        // made, beside what it delivered.
        "runtime": counters.map(|c| serde_json::json!({
            "steps": c.steps,
            "polls": c.polls,
            "waits": c.waits,
            "harvests": c.harvests,
            "spin_hits": c.spin_hits,
            "spin_misses": c.spin_misses,
            "spin_deadlines": c.spin_deadlines,
            "timers_fired": c.timers_fired,
            "park_cost_reads": c.park_cost_reads,
            "park_samples": c.park_samples,
            "wake_samples": c.wake_samples,
            "thread_accounts": c.thread_accounts,
            "completions": c.completions,
        })),
    });
    let _ = std::io::Write::write_all(
        &mut std::io::stdout().lock(),
        format!("{line}\n").as_bytes(),
    );
    Ok(())
}
