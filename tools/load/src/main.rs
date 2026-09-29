//! `focal-load` drives a [`shape::WorkloadShape`] against a node — an
//! `EmbeddedNode` in this process, or a running `focal start` node over its
//! Unix socket — from one or more concurrent callers, and writes a
//! [`report::Report`]. It is the R11 §5 workload generator; the nightly
//! campaign and the capacity envelope are refreshed from its output. A
//! measurement tool, but production code all the same: every failure is a
//! typed error printed once, never an unwind.
mod authored;
mod driver;
mod error;
mod native;
mod report;
mod shape;

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
    let text = std::fs::read_to_string(path)?;
    let shape: shape::WorkloadShape =
        serde_saphyr::from_str(&text).map_err(|error| LoadError::Shape(error.to_string()))?;
    shape.validate().map_err(LoadError::Shape)?;
    Ok(shape)
}

fn run(args: &Args) -> Result<(), LoadError> {
    let shape = read_shape(&args.shape)?;
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
        "committed {} ({} refused, {} unknown) in {} ms from {} worker(s) — {:.0} ops/s, latency p50 {} ns, p99 {} ns{}",
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
    if !report.refusals.is_empty() {
        writeln!(stderr, "refusal reasons: {:?}", report.refusals)?;
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
