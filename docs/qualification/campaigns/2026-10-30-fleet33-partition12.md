<!-- Qualification record: a dozen of 33 focal nodes partitioned, and their connections dropped, under the work flow, 2026-10-30, fedora (Linux x86-64), focal at fleet-run 5a7af464 with the harness at dcf3a798. -->

# A dozen of 33 nodes partitioned, and their connections dropped, under the work flow (goal condition 3 at scale)

Date: 2026-10-30. focal at `fleet-run` 5a7af464, the tree the kill campaign closed on
([a dozen of 33 killed](2026-10-29-fleet33-kill12.md)), its binaries the same image (`focal:fleet-k14`);
the harness at dcf3a798, whose isolation cuts a node off both ways (each run's `tree.txt`: 63ad774c, a
documentation commit on 5a7af464, with that change to `run.sh`).

**Host:** fedora (kiln runner kiln-a094a12d.attlocal.net, hostname fedora-2): Intel Core i9-14900HX
(32 threads), 62.5 GiB, Fedora Linux 44 (kernel 6.19.10), Docker 29.8.2 on btrfs. **Shared:** kiln CI
jobs ran on the host (its load average 3.8 to 15.3 as each run began and 24 to 35 under its load, per
`host.txt`), so the latencies are a busy host's.

**Harness:** `tools/compare/compose/run.sh` in work mode, as the kill campaign runs it, with the same
twelve nodes cut off instead of killed (`FAULT_SEED=1029`):

    NODES=33 NODE_MEMORY=512m MODE=work AGENTS=500 RAISE_HOST_LIMITS=1 \
      FAULTS='isolate=12@45+30' FAULT_SEED=1029 \
      bash tools/compare/compose/run.sh focal 220 120 15 128 OUT     # a partition
    # FAULTS='isolate=12@45+12': their connections dropped

## What a run does

1. **The cluster and the load** as in the kill campaign: 33 nodes of 0.90 CPU and 512 MiB over three
   zones, 11 sessions on three voters in three zones each (placed afresh each run), 500 agents, every
   session a ledger, 220 claims a second for 135 s (120 s measured after 15 s): 29,700 lifecycles.
2. **The fault.** 45 s into the load the twelve (focal1, the founder and the root's and directory
   partition's leader, with focal6, 7, 8, 9, 16, 17, 23, 27, 28, 31 and 32) are each cut off from every
   peer and client at once: every datagram a node sends is lost (netem on its root) and every one sent
   it is dropped as it arrives (an ingress filter), while its process and state go on.
   - **A partition** (`p1`, `p2`): for 30 s. The 21 others and the clients are one side, each of the
     twelve alone; a session is left its majority, if it has one, on the 21's side.
   - **Connections dropped** (`c1`, `c2`): for 12 s, just past focal-wire's QUIC idle timeout (10 s),
     so every connection the twelve hold, to peers and to clients, closes at both ends while the nodes
     run on. focal's transport is QUIC over UDP, which has no reset to send: a path that loses every
     datagram is how a QUIC connection is dropped, as a black-holed route or a lost middlebox does it.
3. **The heal.** The cut is undone at once on all twelve (`rejoined`), every node still running.
4. **The verdict** as in the kill campaign: focal-load's report, the CSV, each node's replicas, health,
   metrics and log end, `killreport.md` by ledger (each ledger's longest time without a commit and where
   it fell), and a sampler of every voter of each session whose preferred leader was cut off, read once
   a second through `docker exec`, which no network fault reaches (`sessions-sampled.txt`).

## Result

| | p1-0227 | p2-0256 | c1-0241 | c2-0310 |
|---|---|---|---|---|
| Fault (cut off by / joined again at) | 30 s (0.1 / 30.4 s) | 30 s (0.1 / 30.4 s) | 12 s (0.1 / 12.3 s) | 12 s (0.1 / 12.3 s) |
| Written and judged, of 29,700 | 29,668 | 29,656 | 29,700 | 29,700 |
| Refused, typed | 32 | 44 | 0 | 0 |
| Unknown / abandoned / undelivered | 0 / 0 / 0 | 0 / 0 / 0 | 0 / 0 / 0 | 0 / 0 / 0 |
| Writes resent under their identity, every one answered | 207 | 177 | 0 | 0 |
| Replicas stopped / sessions not converged | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| Write p50 / p99 (measured window, ms) | 47.3 / 16,735 | 44.8 / 14,954 | 40.4 / 716 | 37.5 / 898 |

**Every lifecycle completed after the heal, or was refused while the twelve were cut off.** Every
refusal is a write begun during the partition: 0.1 to 12.5 s into it in p1, 0.1 to 13.4 s in p2. 29 of
p1's and 36 of p2's were answered before the heal; the other 3 and 8 at the heal itself (within 0.2 and
0.3 s), writes begun in the partition's first second whose 30 s clocks ran out as it healed. The
dropped connections cost no lifecycle: none refused, none resent.

| Cause | Step | p1 | p2 | What refused it |
|---|---|---|---|---|
| `owner_bound` | `validation.begin` | 21 | 21 | its custody check: "N of M required copies hold the artifact; missing nodes …" |
| `unavailable` | `claim.create` | 0 | 11 | the session had no quorum for the client's 30 s clock |
| `unavailable` | `artifact.submit` | 6 | 4 | its node, while it held a content copy cut off (`CopyHealth`) |
| `unavailable` | `validation.report` | 3 | 4 | likewise: the report submits a result artifact |
| `ingress_capacity` | `artifact.submit` | 2 | 4 | the evidence door, its wait at the session's request time up |

**None lost or unknown.** Each resent write was one whose client could not learn its outcome within its
30 s clock; each was answered once. After the load every voter of every session had applied and
committed the same index with no seed chunk or content object missing (all four runs, all 11 sessions);
no replica had stopped, no node held a member dead, and no placement agent was failing: the twelve and
the 21, cut off from each other, revived each other after the heal without a restart.

**No leader on a cut-off node.** A leader cut off from its peers gave up its authority 1.5 to 2.9 s into
the cut, while still cut off, and a voter on the other side led with authority from 1.5 to 2.6 s; a
session left without a majority had no leader until the heal. After the heal each preferred leader
caught up and took its leadership back (`sessions-sampled.txt`).

**Recovery after the heal**, each session's longest time without a commit by what the cut took from it,
with where it fell (all four runs; `killreport.md`), and how soon after the heal it ended:

| Cut off | Sessions | Longest without a commit | Where, from the cut | After the heal |
|---|---|---|---|---|
| nothing | 11 | 0.5 to 0.9 s | anywhere | — |
| a follower holding no content copy (partition), a follower (connections dropped) | 6 | 0.5 to 0.8 s | 0.7 to 1.5 s; 41.8 to 42.6 s | — |
| a follower holding a content copy (partition) | 4 | 14.5 to 15.3 s | 15.3 to 30.7 s | at it (−0.3 to +0.3 s) |
| its leader, holding a content copy (partition) | 4 | 10.0 to 10.1 s, then 8.8 to 13.7 s | 0.1 to 10.2 s; 20.5 to 34.2 s | 0.9 to 3.8 s |
| its leader (connections dropped) | 4 | 10.0 s | 0.1 to 10.1 s | — |
| its quorum (partition) | 7 | 30.5 to 34.0 s | from the cut | 0.2 to 3.7 s |
| its quorum (connections dropped) | 8 | 12.4 to 13.6 s | from the cut | 0.2 to 1.4 s |

The cut-off voters of a session left without its quorum were caught up (within 100 entries of the
session's furthest replica) 0.1 to 0.4 s after the heal; a cut-off leader of a session that went on
without it, 2.4 to 4.7 s after a partition and 1.2 to 2.8 s after a connection drop. A session whose
leader was cut off wrote again 10 s into the cut, when its clients' connections to that leader reached
the QUIC idle timeout. An artifact step needs every content copy of its session (the all-copies custody
rule, 26 §1): while a copy was cut off its jobs waited (or, past their clocks, were refused), and
focal-load's 320 lifecycles a ledger filled at that step, so a ledger whose copy was cut off took no
write from 15 to 23 s into the partition until the copy was back. Every ledger's writes begun from 43
to 92 s after the cut on were none slower than its slowest before it.

## Open

As the kill campaign left them: the all-copies custody rule (an f+1 acknowledgement is the user's
decision, pending); failover detection on the 10 s QUIC idle timeout (a cut-off leader's successor
leads within 2.6 s, its clients find it at 10 s); the evidence door refusing artifact jobs
`ingress_capacity` while their copy is cut off (2 and 4 in the partitions); the snapshot install that
waited on a seed chunk (once in the kill campaign's eleven runs, not here); the directory partition's
authority refresh ceiling (about 500 enrolled clients). Nothing new was found.

## The records

On the host, every file of each run: `fedora-2:~/s5/runs/` (p1-0227, c1-0241, p2-0256, c2-0310). Beside
this record, each run's report, faults log, placement by session and node, host, status, tree, fit,
summary, report by ledger (`killreport.md`) and sampler (`sessions-sampled.txt`), with their SHA-256
sums as the host made them: [`2026-10-30-fleet33-partition12/`](2026-10-30-fleet33-partition12/).
