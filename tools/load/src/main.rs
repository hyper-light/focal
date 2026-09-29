// A workload generator / measurement tool (R11 §5), not shipped. Like the
// dependency-free benches, it is measurement code, so panics on bad input and
// direct stdout are appropriate — hence the allow of the production no-panic
// lints the workspace denies for shipped crates.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::disallowed_macros
)]
//! `focal-load` drives a [`shape::WorkloadShape`] against an in-process node and
//! writes a [`report::Report`]. It is the R11 §5 workload generator; the nightly
//! campaign and the capacity envelope are refreshed from its output.
mod driver;
mod native;
mod report;
mod shape;

use clap::Parser;
use std::path::PathBuf;

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

fn main() {
    let args = Args::parse();
    let text = std::fs::read_to_string(&args.shape)
        .unwrap_or_else(|error| panic!("read {}: {error}", args.shape.display()));
    let shape: shape::WorkloadShape =
        serde_saphyr::from_str(&text).unwrap_or_else(|error| panic!("parse shape: {error}"));
    shape
        .validate()
        .unwrap_or_else(|error| panic!("invalid shape: {error}"));

    let report = driver::run(shape);
    let json = serde_json::to_string_pretty(&report).expect("serialize report");
    match &args.out {
        Some(path) => {
            std::fs::write(path, &json)
                .unwrap_or_else(|error| panic!("write {}: {error}", path.display()));
            eprintln!("wrote {}", path.display());
        }
        None => println!("{json}"),
    }
    eprintln!(
        "committed {} ({} refused, {} unknown) in {} ms — {:.0} ops/s, latency p50 {} ns, p99 {} ns",
        report.committed,
        report.refused,
        report.unknown,
        report.wall_ms,
        report.throughput_ops_per_s,
        report.latency_ns.p50,
        report.latency_ns.p99
    );
}
