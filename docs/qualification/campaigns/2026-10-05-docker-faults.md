<!-- Qualification record: claims under network faults and a SIGKILL on three Docker containers, 2026-10-05, macOS arm64, focal at 6c56b29. -->

# Claims under faults on three containers (goal condition 3)

Date: 2026-10-05. focal at `6c56b29` (`slates-port`), built as `focal:chaos-6c56b29` from
`deploy/container/Dockerfile` (static musl binary on `scratch`, 62.6 MB). Host: Apple Silicon
macOS, Docker Desktop 29.3.1 (linux/arm64, 18 CPUs, 7.6 GiB for the VM). The host was shared
with other sessions; its load average was 50–65 during the run.

Harness: `scripts/chaos/` (`cluster.sh`, `chaos.sh`, `down.sh`). Run with
`bash scripts/chaos/chaos.sh focal:chaos-6c56b29 120 5 50`.

## What the run does

1. **Three nodes.** A founder starts on a bridge network and writes an invitation per host. Each
   host starts with its invitation (`start --invite-file`). The invitation is a secret: focal
   refused one left world-readable by `docker cp` ("private invitation file permissions are
   unsafe"), so the harness writes each into its own volume, owner-only and owned by the node's
   user.
2. **Three voters.** The founder's session is placed to survive one failure
   (`cluster sessions plan --max-failures 1`), and the harness waits until the placement reports
   three voters with nothing pending.
3. **Faults on every node.** A sidecar in each node's network namespace applies
   `tc netem delay 50ms 25ms loss 5%`.
4. **A stream of claims.** 120 claims are created one after another through the founder's CLI
   (`submit claim`). A creation answered `Committed` is a durable claim on a quorum of the
   session's voters.
5. **A voter killed and restarted.** `fc-host-a` is SIGKILLed after claim 40 and started again
   at claim 80.
6. **Verdict.** Every acknowledged claim must read back on the founder and on `fc-host-b`, and
   then on the restarted `fc-host-a`. None may be duplicated.

## Result

| | |
|---|---|
| claims submitted | 120 |
| acknowledged (`Committed`) | 120 |
| refused or outcome unknown | 0 |
| missing on the founder or `fc-host-b` | 0 |
| missing on `fc-host-a` after its restart | 0 (including the 40 committed while it was dead) |
| duplicates | 0 |
| end-to-end per claim (CLI through `docker exec`) | p50 1,177 ms, p99 1,548 ms, max 1,678 ms |

The latency is not focal's commit latency. Each claim starts a CLI process inside the container
through `docker exec`, on a host at load 50–65, with 50 ± 25 ms added to every packet. It is
recorded to bound the run, not to compare. The measured comparison is the
[competitive p99 plan](../competitive-p99.md), on a Linux host.

## What the first two runs taught (harness, not focal)

- A claim targeting the node itself with the action `work` is refused at creation
  (`InvalidPolicy`); the local demo uses `handoff`, as the deployment journeys do.
- Posting a self-handoff claim with only a receipt validation is refused by the native lifecycle
  ("evaluation policy is invalid or unsatisfied"). The run measures the committed creation,
  which is the replicated write.

## Not yet covered

- Killing the founder, through which the CLI writes. A client enrolled at the cluster, writing to
  whichever node leads, is the next step (`focal-load` over QUIC, 19d1c27).
- Partitions (one node cut off both ways), heavier loss (20%) and a kill of the session's leader
  while it is not the founder.
- Artifacts, testaments and validations under the same faults.
- Dozens of nodes. This host's Docker VM holds three comfortably.
