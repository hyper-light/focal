# focal-load

A workload generator for Focal (R11 §5). Not shipped — a measurement tool that
drives a `WorkloadShape` end to end over the real `focal-client` path and writes
a JSON report. The nightly campaign (`tools/load/tests/campaign.rs`), the
capacity envelope (`docs/qualification/capacity-envelope.md`) and the dated
performance records are refreshed from its output. It is production code under
the workspace's no-panic policy: every failure is a typed error printed once.

## Usage

```sh
cargo run -p focal-load --release -- --shape shape.yaml --out report.json
```

`--shape` is required; `--out` defaults to stdout. Summary statistics are always
printed to stderr.

## Shape

```yaml
claims: 1000          # native claim creations to submit end to end (1..=1_000_000)
seed: 7               # optional; deterministic request/claim identities (default 1; < 2^24)
reads: 2000           # optional; claim reads after the creations (default 0; <= 10_000_000)
transport: embedded   # optional; `embedded` (default) or `unix`
data_dir: /tmp/node   # `unix`: the running node's directory; `embedded`: keep the node here
profile: authored_v1  # optional; the frame profile — the transport's own by default
concurrency: 1        # optional; concurrent callers, 1..=64
rate: 100             # optional; fixed offered rate over all callers
warmup_ms: 15000      # optional; intended starts before this time are not measured
reopen: false         # optional, embedded only; reopen the node afterwards and time it
```

`embedded` opens an `EmbeddedNode` in the process (a fresh temporary directory
unless `data_dir` says where), activates it natively (`projection_only` unless
`profile` says `authored_v1`) and drives it over the embedded transport.

`unix` drives a running `focal start` node through `<data_dir>/focal.sock` as
that node's own local participant, exactly as the CLI does without a
`--client-context`; the node must have been activated natively
(`focal cluster replicas activate-native` activates `authored_v1`, the default
frame profile for this transport).

The profile decides how a creation is built. On a `projection_only` ledger a
structural native `Create` (`src/native.rs`, the node's own host-test fixture)
is sent. On an `authored_v1` ledger the codec refuses that form ("creation
profile"), so the same `claim.submit` document the CLI takes — a description,
the subject participant, one required receipt validation with a deadline — is
compiled by the shared native-client compiler (`src/authored.rs`) into the
exact frame the CLI would send, with deterministic identities from the run's
id space. A frame of the wrong profile is refused by the node and counted,
with its reason, under `refusals`.

`concurrency` runs that many callers at once, each an OS thread with its own
client and runtime and its own identity space under the seed; the claims and
then the reads are split evenly among them, so throughput separates from
latency. `reopen` closes the embedded node after the reads, reopens the same
directory and times that to the first linearizable read: single-node recovery
at exactly this run's retained size.

Each run uses a distinct identity space per `seed` (and per worker under it),
so two runs never collide and a run is reproducible.

## Report

```json
{
  "shape": { "claims": 1000, "seed": 7, "reads": 2000, "transport": "embedded",
             "data_dir": null, "profile": null, "concurrency": 1, "reopen": true },
  "committed": 1000, "refused": 0, "unknown": 0,
  "expired": 0, "floors_advanced": 7,
  "wall_ms": 30000,
  "throughput_ops_per_s": 33.3,
  "latency_ns": { "p50": 30000000, "p95": 34000000, "p99": 35000000, "max": 36000000 },
  "latency_ns_first_half": { "p50": 29000000, "p95": 33000000, "p99": 34000000, "max": 36000000 },
  "latency_ns_second_half": { "p50": 31000000, "p95": 35000000, "p99": 35000000, "max": 35500000 },
  "reads": 2000, "read_hits": 2000,
  "read_wall_ms": 200,
  "read_throughput_ops_per_s": 10000.0,
  "read_latency_ns": { "p50": 94000, "p95": 120000, "p99": 150000, "max": 500000 },
  "workers": 1,
  "refusals": [],
  "reopen_ms": 350,
  "first_read_after_reopen_ns": 210000,
  "reopen_read_hit": true,
  "data_dir_bytes": 4194304
}
```

The callers issue their writes in request generations as the CLI's journal
does (the audit's F12): one generation for the run, shared by every worker as
N processes of one participant share one journal, rotated once half the
journal's capacity was issued in it, its floor advanced by the protocol
operation `epoch.advance` once the generation below drained
(`floors_advanced`). A write the owner refused by name — its generation
closed under pressure on the owner's resident window, or not admitted yet —
was never executed: the run reads the owner's window and issues the write
once more in the generation the owner admits (`expired`, counted apart from
`refused`; both attempts are in the samples). `FOCAL_NATIVE_OUTCOMES` on the
node sets the resident window such a run presses on.

Writes are `F_FULLFSYNC`-bound per commit (tens of ms; batching is the throughput
lever — see `crates/focal-log/benches/append.rs`); linearizable reads take no
fsync and run far faster. The two halves are split by each write's start time:
a dearer second half is the per-op cost growing with the session (the
one-session ceiling the capacity envelope states). `read_hits < reads` would
indicate lost committed state — the campaign asserts they are equal; so would
`reopen_read_hit: false`. `refusals` holds at most eight distinct reasons.

`committed`, `refused`, `unknown` and `expired` count the whole write phase,
including warm-up. The original `latency_ns` and half reports describe all
attempts after warm-up, including fast capacity refusals. They therefore do
not describe successful-write latency when any attempts fail or are refused.
`measured_writes` counts that measured population by outcome and reports
`committed_latency_ns`, `refused_latency_ns`, `unknown_latency_ns` and
`expired_latency_ns` separately. Each count covers the same requests as its
percentiles; a zero count means its zero-valued latency has no samples.

## Tail diagnosis

```sh
FOCAL_LOAD_WRITES_CSV=writes.csv cargo run -p focal-load --release -- \
  --shape shape.yaml --out report.json
python3 tools/compare/analyze_tail.py --report report.json --csv writes.csv
```

The CSV retains `start_ns,latency_ns` as its first columns and adds
`sent_ns,finished_ns,worker,write,attempt,epoch,request,outcome,phase_start_ns,run_start_epoch_ns`.
There is one row per attempt after warm-up, sorted by intended start. The
first retained intended start is zero for `start_ns`, `sent_ns` and
`finished_ns`. `phase_start_ns` retains the intended offset from the entire
phase, including warm-up; `run_start_epoch_ns` (also in the JSON as
`write_phase_start_epoch_ns`) approximates that phase's Unix-time origin,
for correlation with checkpoint and host cgroup traces.

`sent_ns` marks entry to `Client::request`; its interval to `finished_ns`
includes client routing and retries and the remote request. Per row,
`latency_ns = finished_ns - start_ns` splits into schedule delay
(`sent_ns - start_ns`) and client request time (`finished_ns - sent_ns`).
The corresponding distributions are `measured_writes.schedule_delay_ns`
and `measured_writes.client_request_latency_ns`. Their percentiles cannot
be added to reconstruct the end-to-end percentile. The request identity,
generation, worker, logical-write index and attempt distinguish re-issues
and let a node-side trace correlate the same request.

The trace reserves at most two samples per logical write before sending,
funded through `focal_memory::MemoryBudget`. Reductions and CSV ordering
scratch are funded separately. CSV output streams through a funded 4 KiB
buffer after the write phase, so writing it does not stall the measured path.

On a Linux Docker host using cgroup v2, `tools/compare/collect_cpu.py` samples
`cpu.max`, `cpu.stat`, pressure, I/O and memory counters for up to eight
containers for a bounded duration. It reads host cgroup files, avoiding a
`docker exec` for every sample; unavailable counters remain explicit. Run
it alongside the generator and correlate its Unix timestamps with the CSV.

## Nightly campaign

`tools/load/tests/campaign.rs` (`#[ignore]`) runs the generator across a seed
range and asserts every run commits its whole workload with nothing refused or
unknown, and that every read observes its committed claim:

```sh
FOCAL_SEED_START=0 FOCAL_SEED_COUNT=16 FOCAL_CAMPAIGN_CLAIMS=500 FOCAL_CAMPAIGN_READS=2000 \
  cargo test -p focal-load --test campaign -- --ignored --nocapture
```

## What it does not do yet

Evidence and artifact transfer, validation modes, retention and geography.
The natural next knobs live in `src/shape.rs` and `src/driver.rs`.
