# focal-load

A workload generator for Focal (R11 §5). Not shipped — a measurement tool that
drives a `WorkloadShape` end to end against an in-process node over the real
`focal-client` path and writes a JSON report. The nightly campaign
(`tools/load/tests/campaign.rs`) and the capacity envelope
(`docs/qualification/capacity-envelope.md`) are refreshed from its output.

## Usage

```sh
cargo run -p focal-load -- --shape shape.yaml --out report.json
```

`--shape` is required; `--out` defaults to stdout. A one-line summary is always
printed to stderr.

## Shape

```yaml
claims: 1000      # native claim creations to submit end to end (1..=1_000_000)
seed: 7           # optional; deterministic request/claim identities (default 1)
reads: 2000       # optional; claim reads after the creations (default 0)
```

Each run uses a distinct identity space per `seed`, so two runs never collide and
a run is reproducible.

## Report

```json
{
  "shape": { "claims": 1000, "seed": 7, "reads": 2000 },
  "committed": 1000, "refused": 0, "unknown": 0,
  "wall_ms": 30000,
  "throughput_ops_per_s": 33.3,
  "latency_ns": { "p50": 30000000, "p95": 34000000, "p99": 35000000, "max": 36000000 },
  "reads": 2000, "read_hits": 2000,
  "read_throughput_ops_per_s": 10000.0,
  "read_latency_ns": { "p50": 94000, "p95": 120000, "p99": 150000, "max": 500000 }
}
```

Writes are `F_FULLFSYNC`-bound per commit (tens of ms; batching is the throughput
lever — see `crates/focal-log/benches/append.rs`); linearizable reads take no
fsync and run far faster. `read_hits < reads` would indicate lost committed
state — the campaign asserts they are equal.

## Nightly campaign

`tools/load/tests/campaign.rs` (`#[ignore]`) runs the generator across a seed
range and asserts every run commits its whole workload with nothing refused or
unknown, and that every read observes its committed claim:

```sh
FOCAL_SEED_START=0 FOCAL_SEED_COUNT=16 FOCAL_CAMPAIGN_CLAIMS=500 FOCAL_CAMPAIGN_READS=2000 \
  cargo test -p focal-load --test campaign -- --ignored --nocapture
```

## Extending

The shape and driver are intentionally minimal. Concurrency, evidence size,
validation mode, retention, geography, and local/QUIC transports are the natural
next knobs (`src/shape.rs`, `src/driver.rs`).
