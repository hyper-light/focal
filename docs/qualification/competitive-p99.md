# Competitive tail latency: plan of record

Goal condition 4 asks that focal's networked delivery keep p99s an order of magnitude
beyond Kafka, Redis HA and NATS, with better correctness. No such claim stands until it
is measured on equal terms. This document fixes those terms before any number is taken;
the results go in `performance/` beside it, with hardware, versions, seeds and loads.

## What is measured

The latency from a producer's send to the system's durable acknowledgement of one
record, replicated on three nodes, at fixed offered rates. The comparison is open-loop:
requests are issued on a fixed schedule, and each latency counts from the request's
intended send time, not from when the client managed to send it. A closed loop slows
itself down when the system stalls and so hides the stall (coordinated omission: Gil
Tene, "How NOT to Measure Latency", and HdrHistogram's correction). Every run records
p50, p99, p99.9 and max in an HdrHistogram (3 significant digits), achieved against
offered throughput, errors and refusals, and each node's CPU and RSS.

## Equal terms

All four systems run as three containers in one Docker Linux VM on the same host, each
container limited to the same CPUs and memory, on the same bridge network. Each runs
twice where its durability is configurable:

| System | As shipped | Each acknowledged write on disk at a quorum |
|---|---|---|
| focal | quorum commit, fsync before acknowledgement | the same (it has no weaker mode) |
| Kafka (KRaft, 3 brokers) | `acks=all`, `min.insync.replicas=2`, flush left to the page cache | add `log.flush.interval.messages=1` |
| NATS JetStream (R3, file storage) | `sync_interval` 2 min | `sync_interval: always` |
| Redis (primary + 2 replicas) | asynchronous replication | `appendfsync always` on every node, then `WAITAOF 1 2 0` per write |

The "as shipped" rows are not equal terms: Jepsen's NATS 2.12.1 analysis (2025)
lost 14.1% of acknowledged writes to a coordinated power failure under the 2-minute
default, a page-cache acknowledgement is lost to the same fault, and Redis's WAIT does
not make replication strongly consistent (Jepsen; the Redis WAIT documentation). They
are run because they are what operators deploy; the claim is made against the
durable rows.

## Conditions

Each system, at each durability, at offered rates of 1k, 4k, 16k and 64k records a
second, payloads of 128 B and 4 KiB, three runs of 60 s after a 15 s warm-up, under:

1. a quiet network;
2. `tc netem` on every node's interface: 2 ms ± 1 ms delay, then 50 ms ± 20 ms, then
   1% and 5% loss;
3. a leader or partition-owner killed mid-run (SIGKILL), measuring the stall and the
   records lost or duplicated against the producer's acknowledged set;
4. the host loaded to the gate's level (CPU burners bounded by their own alarm).

## Correctness beside latency

Each run's producer keeps the set of records acknowledged; a consumer reads every record
back after the run. Lost acknowledged records and duplicates are counted per run and
reported beside the percentiles: a fast acknowledgement that loses data does not count
as faster.

## Findings from the harness's first runs (2026-10-04; not results)

These come from smoke runs with the workspace gate running beside them. They are about
the harness and the host, not about any system's tail:

- **Fsync on Docker Desktop's VM is not a flush to stable storage.** 500 `dd
  oflag=dsync` writes of 4 KiB took about 10 ms in all, about 20 µs each, on both the
  overlay and a named volume. The durable rows therefore measure nothing about disks on
  this host; they run on a Linux host with real drives.
- **A client's own deadline must not truncate the measurement.** async-nats's default
  JetStream ack timeout turned every slow acknowledgement into a failure. The generator
  now sets it to the histogram's highest value, 120 s.
- **NATS JetStream with `sync_interval: always` sustained about 9 synchronous publishes a
  second here** (`nats bench js pub sync`: average 104 ms, p99 416 ms). At 100/s offered
  it queued, p50 5.7 s and p99 11.1 s.
- **Each system needs its canonical high-performance client.** rskafka sends one record
  per produce request and does not pipeline them on a partition, so it capped near
  650/s and its latency was the client's queue, not Kafka's. Kafka is driven through
  librdkafka with its batching and in-flight window, NATS through async-nats, Redis
  through redis-rs. Each measures from each record's intended send time.
- **redis-rs's response timeout did the same to every `WAITAOF`.** Its deadline is now
  120 s as well.
- **The generator runs inside the VM, on the bench network.** Redis as shipped answered
  `SET` at p50 9.5 ms through Docker Desktop's port forwarding from macOS, which adds a
  proxy hop to every request and hides the systems' own differences.
- **The report needs a drain time.** "Achieved per second" counts every acknowledgement
  against the offered window, so a system that acknowledges long after the window
  closes looks as fast as the offered rate. The time from the last offer to the last
  acknowledgement goes in the report.

## Status

Planned 2026-10-04. The generator (`tools/compare`) has run against all three
competitors: Kafka through librdkafka (`acks=all`, idempotence on, librdkafka's default
batching), NATS through async-nats, Redis through a pool of redis-rs connections. Each
report carries its drain time. Remaining before the measured runs:

- **focal's arm.** focal is driven as a remote enrolled client over QUIC, like every
  competitor. `PendingClientJoin::remote_client` (focal-node's library) now opens that
  client, and the CLI's enrolled contexts use the same path. The writes are
  `focal-load`'s authored creations, whose request generations (epochs minted and
  floors advanced) that tool's driver already manages. So `focal-load` gains the
  enrolled transport and an open-loop mode, timed from each request's intended send
  into an HdrHistogram and reported in this generator's schema. Its present
  closed-loop driver times from the actual send, which is not comparable.
- **The cluster.** Done (2026-10-07): `docker compose --profile focal up -d` and
  `focal-bootstrap.sh` (`tools/compare/compose`) found three `focal:bench` containers (both
  binaries, musl, on Alpine for `tc`; 107 MB), one zone each under the competitors' limits;
  the hosts join by invitation, the root, its partition and the session are placed on all
  three surviving a zone (`deployment plan`/`apply`, asked again until the joined hosts
  have reported their load, since a plan made before records the capacity missing and only
  its apply refuses), the ledger is activated natively, and a client is enrolled in the
  generator's container over QUIC. Founding it found a defect, fixed at its cause: a data
  directory left 0755 by Docker started, then failed administration as a corrupt journal
  (focal `2d12e5a`). A first smoke run, not a result: 2,000 authored claims offered at
  200/s over four callers, all committed (none refused or unknown), p50 5.2 ms, p99
  12.2 ms, p99.9 60.9 ms, max 65.9 ms, beside the workspace gate.
- **The generator in a container** on the bench network, and a Linux host with real
  disks.

## First measurements on Linux runners (2026-10-08)

GitHub `ubuntu-24.04` runners: four vCPUs, one virtual disk (`sda`, 150 GB). Its fsync reaches
the device: 500 synchronous 4 KiB writes took 167 ms, about 334 µs each, against about 20 µs
under Docker Desktop. Each system ran as three containers with one CPU share each, beside its
generator, every acknowledged write on disk at a quorum. Open-loop, 128 B records, 15 s warm-up,
60 s measured. Two runs of each system (37755449086, 37759454104), p99 per run:

| System (durable) | 1,000/s p50 | 1,000/s p99 | 4,000/s p50 | 4,000/s p99 |
|---|---|---|---|---|
| Kafka (flush per message) | 5.2–5.3 ms | 9.0–15.3 ms | 5.7–5.9 ms | 12.4–13.8 ms |
| NATS JetStream (sync always) | 2.4–3.0 ms | 3.5–7.4 ms | saturated: 33–39% refused, p99 1.6–1.8 s | — |
| Redis (appendfsync always, WAITAOF 1 2) | 2.4–3.0 ms | 5.8–15.3 ms | 2.4 ms | 4.0–5.7 ms |

The spread between two runs of one system is as wide as the gap between systems, so a claim
needs several seeded runs per condition. Recording a single run is not enough.

**focal has no measured result on the runners yet.** Its arm found three defects, each fixed or
tracked:
- The bootstrap invited before the founder led its root: readiness asks no leadership by design.
  Fixed: it waits on `authoritative`. The rendered Kubernetes invitations script had the same
  gap and is fixed too.
- The load tool was pointed at the client's root instead of its context journal. Fixed.
- At one CPU a node, the ledger session stopped on every replica at a checkpoint (`native session
  checkpoint: native Core checkpoint: native codec capacity exceeded`) after admitting work, and
  the run then waited without end. A session must refuse at admission, never stop at a
  checkpoint. Admission projects the checkpoint's rows and bytes but not its encoder's work
  bound. Not yet reproduced off the runner: open.

**focal's replicated write is slow.** Three local processes on an Apple-silicon Mac (macOS's full
flush, which costs more than Linux's fdatasync), enrolled over QUIC, unloaded (20/s, 2 callers):
p50 42 ms, p99 114 ms. At 1,000/s offered, 10 paced callers achieved 124/s. The competitors' p50
on the runners was 2.4–5 ms. A floor of tens of milliseconds at low load is a cost in the
request's path, not the disk's or the network's, and it is the first thing to find and remove.

**A session's open claims are bounded** (about 11,800 authored claims before admission refuses,
`native owner memory`). The competitors append to a log without state. focal's write is a
claim's state transition, which stays open until it is closed, so a long run at a high rate
fills one session. The comparison will state focal's unit plainly and spread load over sessions
as a deployment would, never hide the bound.

