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

### Harsher: 20% loss, 100 ± 50 ms on every node

`bash scripts/chaos/chaos.sh focal:chaos-6c56b29 120 20 100`, the same kill and restart:

| | |
|---|---|
| acknowledged | 120 of 120 |
| refused or outcome unknown | 0 |
| missing on any of the three nodes, the restarted one included | 0 |
| duplicates | 0 |
| end-to-end per claim | p50 1,543 ms, p99 2,291 ms, max 2,351 ms |

### Partition: a voter cut off both ways, then reconnected

`bash scripts/chaos/chaos.sh focal:chaos-6c56b29 120 5 50 partition`: `fc-host-b` loses every
packet in and out from claim 40 to claim 80 while it keeps running, then returns to the run's 5%
loss. The node is alive throughout, so its timers fire while it hears nothing.

| | |
|---|---|
| acknowledged | 120 of 120 |
| refused or outcome unknown | 0 |
| missing on any of the three nodes, the reconnected one included | 0 |
| duplicates | 0 |
| end-to-end per claim | p50 1,238 ms, p99 1,515 ms, max 1,584 ms |

The fault sidecar is a local image with `tc` already installed (`scripts/chaos/tc.Dockerfile`).
A sidecar that installed it at run time would fetch over the node's own network, and a node cut
off could never be reconnected.

### The session's leader cut off: refused, never lost, resolved exactly once

`bash scripts/chaos/chaos.sh focal:chaos-6c56b29 120 5 50 leader`: the founder, which leads the
session and is the node the CLI writes through, loses every packet from claim 40 to claim 80.

| claims | outcome |
|---|---|
| 1–39 | acknowledged |
| 40–79 (founder cut off) | all 40 refused `unavailable`; none acknowledged |
| 80–82 (at reconnection) | `RequestUnconfirmed`, `route_changed`: the session's route moved to epoch 2 while the founder was away; the CLI names `request retry --operation-id n1:…` for each |
| 83–120 | acknowledged |

After the run:

- **Every acknowledged claim (77) reads back on all three nodes;** none missing, none duplicated.
- **Each unconfirmed operation resolves exactly once.** `request retry` answered each `Committed`
  at sequences 78, 79 and 80, and a second retry answered the same sequence and the same claim.
  Each of the three reads back on all three nodes.
- **No refused write committed behind the client.** The 77 acknowledged claims and the three
  retried ones are sequences 1–80 exactly, so none of the 40 `unavailable` writes was committed.

While the founder was cut off, the other two voters still made a majority. Writes stopped
because the CLI here writes through the founder's own local socket. A client enrolled at the
cluster would follow the route to the new leader; that is the next run.

### The leader cut off, written through an enrolled client: correct, not available

`WRITER=client bash scripts/chaos/chaos.sh focal:chaos-6c56b29 120 5 50 leader`: the same cut,
but every claim is written by participant `alice`, enrolled at the cluster (`cluster client
invite`, `context enroll`), over QUIC from a container of its own.

| claims | outcome |
|---|---|
| 1–39, 80–120 | 80 acknowledged; every one reads back on all three nodes; none duplicated |
| 40–79 (founder cut off) | all 40 `transport unavailable`: the client never reached a node |

**Correct, and not available.** Nothing acknowledged was lost and nothing was written twice, but
while the founder was cut off the client wrote nothing, although the two other voters were a
majority. The client knows one endpoint, the one its invitation names
(`PendingClientJoin::remote_client` seeds its route with the invitation's endpoint), and learns
another only from a `RouteChanged` answer, which only a reachable node can give. A client of a
replicated service is given a bounded set of members to try on a transport failure: Kafka's
`bootstrap.servers`, etcd's endpoint list, NATS's server pool. A member reached that does not
serve the route answers with its route, which the client already follows. This is the next fix.

## What the first two runs taught (harness, not focal)

- A claim targeting the node itself with the action `work` is refused at creation
  (`InvalidPolicy`); the local demo uses `handoff`, as the deployment journeys do.
- Posting a self-handoff claim with only a receipt validation is refused by the native lifecycle
  ("evaluation policy is invalid or unsatisfied"). The run measures the committed creation,
  which is the replicated write.

## Not yet covered

- Killing the founder, through which the CLI writes. A client enrolled at the cluster, writing to
  whichever node leads, is the next step (`focal-load` over QUIC, 19d1c27).
- An enrolled client that fails over to another member while the founder is cut off (the gap
  above), and a kill of the session's leader while it is not the founder.
- Artifacts, testaments and validations under the same faults.
- Dozens of nodes. This host's Docker VM holds three comfortably.
