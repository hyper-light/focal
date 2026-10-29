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
//! `focal-load` drives a [`shape::WorkloadShape`] against a node — an
//! `EmbeddedNode` in this process, or a running `focal start node` node over its
//! Unix socket — from one or more concurrent callers, and writes a
//! [`report::Report`]. It is the R11 §5 workload generator; the nightly
//! campaign and the capacity envelope are refreshed from its output. A
//! measurement tool, but production code all the same: every failure is a
//! typed error printed once, never an unwind.
mod authored;
mod driver;
mod error;
mod generations;
mod measurements;
mod native;
mod progress;
mod report;
mod seats;
mod shape;
mod watch;

use clap::Parser;
use error::LoadError;
use std::io::Write as _;
use std::path::PathBuf;
use std::process::ExitCode;

/// The most bytes a shape file may hold.
const MAX_SHAPE_BYTES: u64 = 64 * 1024;

#[derive(Parser)]
#[command(name = "focal-load", about = "Focal workload generator (R11 §5)")]
struct Args {
    /// Workload shape (YAML): `claims`, and optionally `seed`, `reads`,
    /// `transport`, `data_dir`, `profile`, `concurrency`, `reopen`.
    #[arg(long)]
    shape: PathBuf,
    /// Report destination (JSON). Printed to stdout when omitted.
    #[arg(long)]
    out: Option<PathBuf>,
}

fn read_shape(path: &PathBuf) -> Result<shape::WorkloadShape, LoadError> {
    let length = std::fs::metadata(path)?.len();
    if length > MAX_SHAPE_BYTES {
        return Err(LoadError::Shape(format!(
            "{} is {length} bytes; a shape is at most {MAX_SHAPE_BYTES}",
            path.display()
        )));
    }
    // The length is read again through the bound: a file that grew after
    // its metadata was read is refused, not read whole.
    let mut text = String::new();
    std::io::Read::read_to_string(
        &mut std::io::Read::take(
            std::fs::File::open(path)?,
            MAX_SHAPE_BYTES.saturating_add(1),
        ),
        &mut text,
    )?;
    if u64::try_from(text.len()).map_or(true, |read| read > MAX_SHAPE_BYTES) {
        return Err(LoadError::Shape(format!(
            "{} grew past {MAX_SHAPE_BYTES} bytes while it was read",
            path.display()
        )));
    }
    let shape = parse_shape(&text)?;
    shape.validate().map_err(LoadError::Shape)?;
    Ok(shape)
}

/// A shape under the same budget as every YAML document focal reads: no
/// aliases or anchors, so no document expands past the bytes it holds.
fn parse_shape(text: &str) -> Result<shape::WorkloadShape, LoadError> {
    let options = serde_saphyr::options! {
        budget: serde_saphyr::budget! {max_depth:16,max_events:8192,max_nodes:4096,max_total_scalar_bytes:64*1024,max_aliases:0,max_anchors:0,max_documents:1},
    };
    serde_saphyr::from_str_with_options(text, options)
        .map_err(|error| LoadError::Shape(error.to_string()))
}

fn run(args: &Args) -> Result<(), LoadError> {
    let shape = read_shape(&args.shape)?;
    // A run among enrolled agents holds a socket for each agent's connection: as many as the
    // kernel lets the process hold, as the node takes (`focal_platform::raise_open_files`).
    let _ = focal_platform::raise_open_files();
    let report = driver::run(shape)?;
    let json = serde_json::to_string_pretty(&report)?;
    let mut stderr = std::io::stderr().lock();
    match &args.out {
        Some(path) => {
            std::fs::write(path, &json)?;
            writeln!(stderr, "wrote {}", path.display())?;
        }
        None => {
            let mut stdout = std::io::stdout().lock();
            stdout.write_all(json.as_bytes())?;
            stdout.write_all(b"\n")?;
            stdout.flush()?;
        }
    }
    writeln!(
        stderr,
        "committed {} ({} refused, {} unknown) in {} ms from {} worker(s) — {:.0} ops/s, all attempts latency p50 {} ns, p99 {} ns{}",
        report.committed,
        report.refused,
        report.unknown,
        report.wall_ms,
        report.workers,
        report.throughput_ops_per_s,
        report.latency_ns.p50,
        report.latency_ns.p99,
        match report.reopen_ms {
            Some(millis) => format!("; reopened in {millis} ms"),
            None => String::new(),
        }
    )?;
    writeln!(
        stderr,
        "measured writes: {} committed, {} refused, {} unknown, {} expired; successful-write p99 {} ns, schedule-delay p99 {} ns, client-request p99 {} ns",
        report.measured_writes.committed,
        report.measured_writes.refused,
        report.measured_writes.unknown,
        report.measured_writes.expired,
        report.measured_writes.committed_latency_ns.p99,
        report.measured_writes.schedule_delay_ns.p99,
        report.measured_writes.client_request_latency_ns.p99,
    )?;
    if !report.refusals.is_empty() {
        writeln!(stderr, "refusal reasons: {:?}", report.refusals)?;
    }
    // Every ledger the run drove, then their totals: at most one a caller.
    let rows = report
        .ledgers
        .iter()
        .map(|row| {
            (
                format!("ledger {}/{}", row.ledger.tenant, row.ledger.session),
                &row.counts,
            )
        })
        .chain(std::iter::once(("all ledgers".to_owned(), &report.totals)));
    for (name, counts) in rows {
        let causes = if counts.refused > 0 {
            format!(" ({:?})", counts.refused_by_cause)
        } else {
            String::new()
        };
        writeln!(
            stderr,
            "{name}: lifecycles {} started, {} written, {} judged; {} refused{causes}, {} unknown, {} abandoned, {} undelivered",
            counts.started,
            counts.written,
            counts.judged,
            counts.refused,
            counts.unknown,
            counts.abandoned,
            counts.undelivered,
        )?;
    }
    Ok(())
}

fn main() -> ExitCode {
    let args = Args::parse();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            // A closed stderr leaves nothing else to say; the exit code stands.
            let _ = writeln!(std::io::stderr().lock(), "focal-load: {error}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A deliver shape names two principals and the authored profile, checked before any work.
    #[test]
    fn a_deliver_shape_is_checked_before_any_work() {
        let shape = |text: &str| parse_shape(text).unwrap().validate();
        let deliver = "claims: 4\nlifecycle: deliver\nprofile: authored_v1\n";
        assert!(shape(deliver).is_ok());
        // The embedded node's own profile is projection_only: deliver compiles authored steps.
        assert!(shape("claims: 4\nlifecycle: deliver\n").is_err());
        assert!(shape(&format!("{deliver}transport: unix\ndata_dir: /tmp/x\n")).is_err());
        let enrolled = format!("{deliver}transport: enrolled\nenrollment: /c\n");
        assert!(shape(&enrolled).is_err(), "no consumer");
        assert!(shape(&format!("{enrolled}consumer_enrollment: /d\n")).is_ok());
        assert!(
            shape(&format!(
                "{enrolled}consumer_enrollment: /d\nworker: \"{}\"\n",
                "1".repeat(32)
            ))
            .is_err(),
            "a deliver run's claims target its consumer"
        );
        assert!(shape(&format!("{deliver}poll_ms: 0\n")).is_err());
        assert!(shape(&format!("{deliver}poll_ms: 1001\n")).is_err());
        // The inbox read's wait at the node: none (polling), up to the tool's bound.
        assert!(shape(&format!("{deliver}inbox_wait_ms: 0\n")).is_ok());
        assert!(shape(&format!("{deliver}inbox_wait_ms: 20000\n")).is_ok());
        assert!(shape(&format!("{deliver}inbox_wait_ms: 20001\n")).is_err());
        assert!(shape("claims: 4\nconsumer_enrollment: /d\n").is_err());
    }

    #[test]
    fn a_shape_that_expands_by_aliases_is_refused() {
        assert!(parse_shape("claims: 10\nseed: 3\n").is_ok());
        // Each level names the one before nine times: 9^5 claims' worth of
        // nodes from a few hundred bytes (the "billion laughs" shape).
        let mut bomb = String::from("a: &a [1,1,1,1,1,1,1,1,1]\n");
        for (level, previous) in ["b", "c", "d", "e"].iter().zip(["a", "b", "c", "d"]) {
            let refs = vec![format!("*{previous}"); 9].join(",");
            bomb.push_str(&format!("{level}: &{level} [{refs}]\n"));
        }
        bomb.push_str("claims: 1\n");
        assert!(parse_shape(&bomb).is_err());
        // An anchor and an alias on fields the shape takes: the default
        // budget expands them, this one refuses them.
        assert!(parse_shape("claims: &n 5\nseed: *n\n").is_err());
        assert!(serde_saphyr::from_str::<shape::WorkloadShape>("claims: &n 5\nseed: *n\n").is_ok());
    }

    /// Enrolled agents may be homed over a cluster's nodes, at most one home an agent; homes
    /// apply to nothing else.
    #[test]
    fn homes_apply_to_enrolled_agents_at_most_one_an_agent() {
        let shape = |text: &str| parse_shape(text).unwrap().validate();
        let work = "claims: 4\nlifecycle: work\nprofile: authored_v1\ntransport: enrolled\n\
                    enrollment: /c\nagents: 3\nagent_enrollments: /a\n";
        let home = |n: u16| format!("  - endpoint: \"focal{n}:7443\"\n    server_name: \"n{n}\"\n");
        assert!(shape(work).is_ok());
        assert!(shape(&format!("{work}homes:\n{}", home(1))).is_ok());
        assert!(shape(&format!("{work}homes:\n{}{}{}", home(1), home(2), home(3))).is_ok());
        assert!(
            shape(&format!(
                "{work}homes:\n{}{}{}{}",
                home(1),
                home(2),
                home(3),
                home(4)
            ))
            .is_err(),
            "more homes than agents"
        );
        assert!(shape(&format!("{work}homes: []\n")).is_err());
        // Agents on the embedded node have no nodes to be homed on.
        let embedded = "claims: 4\nlifecycle: work\nprofile: authored_v1\nagents: 3\n";
        assert!(shape(embedded).is_ok());
        assert!(shape(&format!("{embedded}homes:\n{}", home(1))).is_err());
        assert!(shape(&format!("claims: 4\nhomes:\n{}", home(1))).is_err());
    }

    /// A run among agents divides over its ledgers: each driven by a caller of its own, among
    /// agents of its own enough for its lifecycle; a session named on enrolled, once, and none on
    /// embedded; homes a ledger's own, and at most one an agent of it.
    #[test]
    fn ledgers_divide_a_run_among_agents() {
        let shape = |text: &str| parse_shape(text).unwrap().validate();
        let session = |n: u8| format!("\"{}\"", format!("{n:02x}").repeat(16));
        let home = |n: u16| {
            format!("      - endpoint: \"focal{n}:7443\"\n        server_name: \"n{n}\"\n")
        };
        let work = "claims: 8\nlifecycle: work\nprofile: authored_v1\nconcurrency: 4\n";
        let embedded = format!("{work}agents: 6\n");
        assert!(shape(&format!("{embedded}ledgers: [{{}}, {{}}]\n")).is_ok());
        assert!(shape(&format!("{embedded}ledgers: [{{}}]\n")).is_ok());
        assert!(shape(&format!("{embedded}ledgers: []\n")).is_err(), "none");
        // Each ledger is a node the run opens: it names no session, and has no nodes to home on.
        assert!(
            shape(&format!(
                "{embedded}ledgers:\n  - session: {}\n",
                session(1)
            ))
            .is_err()
        );
        assert!(shape(&format!("{embedded}ledgers:\n  - homes:\n{}", home(1))).is_err());
        // Three agents a work ledger at least: six over three ledgers is two each.
        assert!(shape(&format!("{embedded}ledgers: [{{}}, {{}}, {{}}]\n")).is_err());
        let deliver =
            "claims: 8\nlifecycle: deliver\nprofile: authored_v1\nconcurrency: 4\nagents: 6\n";
        assert!(shape(&format!("{deliver}ledgers: [{{}}, {{}}, {{}}]\n")).is_ok());
        // A caller a ledger at least, and a lifecycle a ledger.
        let five = "ledgers: [{}, {}, {}, {}, {}]\n";
        assert!(
            shape(&format!("{work}agents: 15\n{five}")).is_err(),
            "5 ledgers, 4 callers"
        );
        let many = format!(
            "claims: 3\nlifecycle: work\nprofile: authored_v1\nconcurrency: 8\nagents: 15\n{five}"
        );
        assert!(shape(&many).is_err(), "5 ledgers, 3 claims");
        // Ledgers apply to a run among agents, of one ledger's reads, reopen and data directory.
        assert!(shape("claims: 8\nconcurrency: 2\nledgers: [{}, {}]\n").is_err());
        assert!(shape(&format!("{embedded}reads: 4\nledgers: [{{}}, {{}}]\n")).is_err());
        assert!(shape(&format!("{embedded}reopen: true\nledgers: [{{}}, {{}}]\n")).is_err());
        assert!(shape(&format!("{embedded}data_dir: /n\nledgers: [{{}}, {{}}]\n")).is_err());
        // Enrolled: each a session of its own, each with homes of its own, one an agent at most.
        let enrolled = format!(
            "{work}transport: enrolled\nenrollment: /c\nagents: 6\nagent_enrollments: /a\n"
        );
        let two = format!(
            "{enrolled}ledgers:\n  - session: {}\n    homes:\n{}{}{}  - session: {}\n",
            session(1),
            home(1),
            home(2),
            home(3),
            session(2)
        );
        assert!(shape(&two).is_ok(), "{:?}", shape(&two));
        let unnamed = format!(
            "{enrolled}ledgers:\n  - session: {}\n  - {{}}\n",
            session(1)
        );
        assert!(shape(&unnamed).is_err(), "a ledger with no session");
        let twice = format!(
            "{enrolled}ledgers:\n  - session: {}\n  - session: {}\n",
            session(1),
            session(1)
        );
        assert!(shape(&twice).is_err(), "one session named twice");
        let malformed = format!(
            "{enrolled}ledgers:\n  - session: \"0x01\"\n  - session: {}\n",
            session(2)
        );
        assert!(shape(&malformed).is_err());
        let crowded = format!(
            "{enrolled}ledgers:\n  - session: {}\n    homes:\n{}{}{}{}  - session: {}\n",
            session(1),
            home(1),
            home(2),
            home(3),
            home(4),
            session(2)
        );
        assert!(shape(&crowded).is_err(), "four homes for three agents");
        // Homes are each ledger's own over ledgers.
        let top = format!(
            "{enrolled}homes:\n{}ledgers:\n  - session: {}\n  - session: {}\n",
            home(1).replacen("    ", "", 2),
            session(1),
            session(2)
        );
        assert!(shape(&top).is_err());
        let one = parse_shape(&two).unwrap();
        assert_eq!(one.ledger_count().unwrap(), 2);
        assert_eq!(one.homes_of(0).map(<[_]>::len), Some(3));
        assert_eq!(one.homes_of(1), None);
    }

    /// A caller reads as many of its lifecycles' steps at once as it writes, unless the shape
    /// says fewer: one read a caller paced 500 agents' work by the round trip of sixteen reads.
    #[test]
    fn a_callers_lifecycle_reads_are_its_writes_unless_set() {
        let unset = parse_shape("claims: 10\ninflight: 64\n").unwrap();
        assert_eq!(unset.lifecycle_reads(), 64);
        assert!(unset.validate().is_ok());
        let set = parse_shape("claims: 10\ninflight: 64\nlifecycle_reads: 4\n").unwrap();
        assert_eq!(set.lifecycle_reads(), 4);
        assert!(set.validate().is_ok());
        let past = parse_shape("claims: 10\ninflight: 4\nlifecycle_reads: 5\n").unwrap();
        assert!(past.validate().is_err());
        let none = parse_shape("claims: 10\ninflight: 4\nlifecycle_reads: 0\n").unwrap();
        assert!(none.validate().is_err());
    }
}
