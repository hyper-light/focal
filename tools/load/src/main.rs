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

#[cfg(test)]
mod tests {
    use super::*;

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
}
