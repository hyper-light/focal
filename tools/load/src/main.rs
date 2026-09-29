//! `focal-load` drives a [`shape::WorkloadShape`] against an in-process node and
//! writes a [`report::Report`]. It is the R11 §5 workload generator; the nightly
//! campaign and the capacity envelope are refreshed from its output.
//!
//! Measurement code is production code (CLAUDE.md §1): a bad input, a node
//! that will not open or a report that cannot be written is a typed error and
//! an exit code, never a panic, and nothing here indexes or adds unchecked.
mod driver;
mod native;
mod report;
mod shape;

use clap::Parser;
use std::fmt;
use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;

#[derive(Parser)]
#[command(name = "focal-load", about = "Focal workload generator (R11 §5)")]
struct Args {
    /// Workload shape (YAML): `claims`, optional `seed`.
    #[arg(long)]
    shape: PathBuf,
    /// Report destination (JSON). Printed to stdout when omitted.
    #[arg(long)]
    out: Option<PathBuf>,
}

/// Why a run did not produce its report.
#[derive(Debug)]
enum LoadError {
    Read(PathBuf, std::io::Error),
    Parse(String),
    Shape(String),
    Run(driver::RunError),
    Report(serde_json::Error),
    Write(PathBuf, std::io::Error),
    Output(std::io::Error),
}
impl fmt::Display for LoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read(path, error) => write!(f, "read {}: {error}", path.display()),
            Self::Parse(error) => write!(f, "parse shape: {error}"),
            Self::Shape(error) => write!(f, "invalid shape: {error}"),
            Self::Run(error) => write!(f, "{error}"),
            Self::Report(error) => write!(f, "serialize report: {error}"),
            Self::Write(path, error) => write!(f, "write {}: {error}", path.display()),
            Self::Output(error) => write!(f, "output: {error}"),
        }
    }
}
impl From<driver::RunError> for LoadError {
    fn from(error: driver::RunError) -> Self {
        Self::Run(error)
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            let _ = writeln!(std::io::stderr().lock(), "focal-load: {error}");
            ExitCode::from(2)
        }
    }
}

fn run() -> Result<(), LoadError> {
    let args = Args::parse();
    let text = std::fs::read_to_string(&args.shape)
        .map_err(|error| LoadError::Read(args.shape.clone(), error))?;
    let shape: shape::WorkloadShape =
        serde_saphyr::from_str(&text).map_err(|error| LoadError::Parse(error.to_string()))?;
    shape.validate().map_err(LoadError::Shape)?;

    let report = driver::run(shape)?;
    let json = serde_json::to_string_pretty(&report).map_err(LoadError::Report)?;
    let mut stderr = std::io::stderr().lock();
    match &args.out {
        Some(path) => {
            std::fs::write(path, &json).map_err(|error| LoadError::Write(path.clone(), error))?;
            writeln!(stderr, "wrote {}", path.display()).map_err(LoadError::Output)?;
        }
        None => {
            let mut stdout = std::io::stdout().lock();
            stdout
                .write_all(json.as_bytes())
                .and_then(|()| stdout.write_all(b"\n"))
                .map_err(LoadError::Output)?;
        }
    }
    writeln!(
        stderr,
        "committed {} ({} refused, {} unknown) in {} ms — {:.0} ops/s, latency p50 {} ns, p99 {} ns",
        report.committed,
        report.refused,
        report.unknown,
        report.wall_ms,
        report.throughput_ops_per_s,
        report.latency_ns.p50,
        report.latency_ns.p99
    )
    .map_err(LoadError::Output)
}
