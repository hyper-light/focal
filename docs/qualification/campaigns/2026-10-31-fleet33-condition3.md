<!-- Qualification record: goal condition 3 at scale, a dozen of 33 focal nodes killed, partitioned and cut off under the work flow, 2026-10-31 to 2026-11-01, fedora (Linux x86-64), the final runs at fleet-run c606af7b (m4 with the harness at 902acac7), every fix the runs found at 6bb00314, confirmed at 33 nodes 2026-11-01 and 02 (m6, k28, m7, k29, m8, k30 to k33, c18, p10), with what the confirmation found fixed at 832a0567; every lifecycle completed with agents that retry as focal's contract says, 2026-11-02 (905cafba: k34, m9, c19, p11); every lifecycle completed with agents that send each write once, 2026-11-03 (focal:fleet-c23, feba1336: m13, k37, c23, p14; before it k35, m10, k36, m11, m12, c21, p13). -->

# Condition 3 at scale: a dozen of 33 nodes killed, partitioned and cut off under the work flow

Dates: 2026-10-31 to 2026-11-02. The final runs at `fleet-run` c606af7b (image `focal:fleet-c15`),
the tree on which the defects that [the kill campaign](2026-10-29-fleet33-kill12.md), [the
partition campaign](2026-10-30-fleet33-partition12.md) and the runs below found close; m4 ran the
same image with the harness at 902acac7, which draws a fault's nodes from those outside the control
plane (below). What the final runs found closes at five commits: 01a2f923 (a session the controller
could not act on stopped the control of every session after it), 22132325 (a node whose route was
older than its client's sent the client back to it), baabea7d (focal-load: an agent asks again a
read a lagging replica answered), c63c52c1 (the bootstrap asks an invitation again) and 6bb00314 (a
join asks again a sponsor whose name did not resolve). Six runs at 33 nodes confirm them (m6 and
k28 on that tree, m7 and k29 with b9f8a514, m8 and k30 with 832a0567). Those runs found two more
defects, fixed at the cause in b9f8a514 and 832a0567 and confirmed in m8 and k30 (below). k31 to
k33 ran k28's fault three more times on that tree, c18 cut a dozen off past the QUIC idle timeout,
and p10 partitioned a dozen for 30 s, so every fault kind ran on the tree with every fix. Every
lifecycle in those runs was written and judged or refused with a typed error, and their claims,
artifacts, testaments and validations are counted by kind (below). With focal-load's agents
sending a refused write again as focal's agent contract says (905cafba), four more runs on that
tree's nodes completed every lifecycle, one of each fault kind: a dozen killed for 30 s and, drawn
outside the control plane, for 60 s, cut off for 12 s and partitioned for 30 s, every claim,
artifact, testament and validation committed (below). With focal itself carrying a write through
an outage instead (the client's delivery bound, 8cc84355; the custody rule by kind of object,
0388d9b2; a heal withdrawn only before its copies begin to join, feba1336), four runs on
`focal:fleet-c23` completed every lifecycle with agents that send each write once, one of each
fault kind (below). ab1630ba corrects two comments. The harness (`tools/compare/compose`) reads every
node's replicas of the sessions its agent installed during the run as well (each run's
`tree.txt`).

**Host:** fedora (kiln runner kiln-a094a12d.attlocal.net, hostname fedora-2): Intel Core i9-14900HX
(32 threads), 62.5 GiB, Fedora Linux 44, Docker's root (every node's volume) on the host's root
btrfs volume (`compress=zstd:1`, one NVMe). **Shared:** kiln CI jobs and other builds ran on the
host throughout (each run's `host.txt`); each run began only once the host's one-minute load was
under 16 and 24 GiB were available (`~/s5/campaign.sh`), so the latencies are a busy host's.

**Harness:** `tools/compare/compose/run.sh` in work mode as the two campaigns before ran it: 33
nodes of 0.90 CPU and 512 MiB over three zones, 11 sessions on three voters in three zones each
(placed afresh each run), 500 agents, every session a ledger, 220 claims a second for 135 s (120 s
measured after 15 s): 29,700 lifecycles a run. The faults, 45 s into the load:

    NODES=33 NODE_MEMORY=512m MODE=work AGENTS=500 RAISE_HOST_LIMITS=1 FAULT_SEED=1029 \
      FAULTS='kill=12@45+30'      # twelve killed and started again 30 s later
      FAULTS='isolate=12@45+30'   # twelve cut off from every peer and client for 30 s
      FAULTS='isolate=12@45+12'   # for 12 s, past the QUIC idle timeout: every connection dropped
      FAULTS='kill=12-control@45+30'  # twelve of the nodes holding no root or partition vote
      bash tools/compare/compose/run.sh focal 220 120 15 128 OUT

The seed draws the same twelve every time (focal1, 6, 7, 8, 9, 16, 17, 23, 27, 28, 31 and 32): the
founder and the root's leader among them. The directory partition's other voters are drawn at each
bootstrap, so the twelve took one of its voters in c16 (of four), p9, c17 and k27 (of three), where
the partition kept its quorum and went on placing, and two in k26 (of four), where it placed nothing
while they were down. m3 killed a dozen that spare the founder (`kill=focal2,focal6,…,focal32@45+30`,
the same set with focal2 for focal1), meant to keep the partition placing so that sessions move off
their dead voters under the load, but its voters beside the founder were focal2 and focal6; m4
drew its twelve from the nodes holding no vote in the root's group or a partition's (902acac7),
leaving the control plane every voter.

## Result

Seven runs on the final image (`focal:fleet-c15`, c606af7b; m4 with the harness at 902acac7), each
of 29,700 lifecycles:

| | c16-0602 | k26-0620 | p9-0647 | m3-0704 | c17-0721 | k27-0749 | m4-0816 |
|---|---|---|---|---|---|---|---|
| Fault | all cut off by 0.1 s, joined again at 12.3 s | all killed by 2.4 s, started again at 37.9 s | all cut off by 0.1 s, joined again at 30.3 s | all killed by 2.2 s, started again at 38.3 s | all cut off by 0.1 s, joined again at 12.3 s | all killed by 11.0 s, started again at 72.5 s | all killed by 11.2 s, started again at 47.7 s |
| Written and judged, of 29,700 | 29,632 | 29,593 | 29,594 | 29,633 | 29,626 | 27,954 | 28,595 |
| Refused, typed | 68 (`owner_bound` 68) | 107 (`owner_bound` 40, `unavailable` 67) | 106 (`owner_bound` 76, `unavailable` 30) | 66 (`owner_bound` 32, `unavailable` 34) | 74 (`owner_bound` 74) | 1,742 (`owner_bound` 219, `unavailable` 1523) | 1,105 (`owner_bound` 24, `unavailable` 1081) |
| Unknown / abandoned / undelivered | 0 / 0 / 0 | 0 / 0 / 0 | 0 / 0 / 0 | 0 / 1 / 0 | 0 / 0 / 0 | 1 / 3 / 0 | 0 / 0 / 0 |
| Writes resent under their identity | 33 | 1338 | 717 | 1218 | 31 | 4283 | 1851 |
| Write p50 / p99 (measured, ms) | 113.7 / 589.0 | 113.9 / 16,012.9 | 105.3 / 16,306.7 | 118.6 / 14,886.2 | 91.9 / 582.9 | 905.7 / 46,926.5 | 152.4 / 22,192.5 |
| Longest with no commit in any session (s) | 0.1 | 0.1 | 1.1 | 0.1 | 0.1 | 4.8 | 1.4 |
| Longest gap after the fault, any session (s) | 0.6 | 0.8 | 1.2 | 0.4 | 0.5 | 7.4 | 0.5 |
| Sessions stopped / nodes holding a member dead / agents failing, at the end | 0 / 0 / 0 | 0 / 0 / 0 | 0 / 0 / 0 | 0 / 0 / 0 | 0 / 0 / 0 | 0 / 0 / 0 | 0 / 0 / 0 |

Every refusal is the all-copies custody rule's or a session's without its quorum, read from the
report's steps: `owner_bound` at `validation.begin` (the custody check, "N of M required copies hold
the artifact") and `unavailable` at `artifact.submit` and `validation.report` (artifact steps held to
the client's 30 s clock while a content copy was down) are the rule's, and `unavailable` at
`claim.create` falls only in the sessions without their quorum (k26: 2, k27: 171, m4: 265). In the
12 s cuts every refusal is a custody check at `validation.begin` (68 and 74), begun during the cut,
nearly all in the sessions whose leader was cut off (below). m3's one abandoned lifecycle and k27's
three are reads a lagging replica answered `Behind`; k27's one unknown write is a node's route older
than its client's (both found and fixed below).

The longest window each session went without a commit, by what the fault took from it (sessions:
the range of their longest windows within the fault; for a session without its quorum, when it
wrote again against the fault's undoing):

| Lost | c16-0602 | k26-0620 | p9-0647 | m3-0704 | c17-0721 | k27-0749 | m4-0816 |
|---|---|---|---|---|---|---|---|
| nothing | 3: 0.4–0.5 s | 4: 0.4–0.5 s | 3: 0.9 s | 3: 0.5 s | 3: 0.5 s | 1: 3.9 s | 4: 1.4–3.0 s |
| a follower | 3: 0.4–0.6 s | 2: 0.4–0.5 s | 1: 0.9 s | 4: 0.5 s | 3: 0.4–0.5 s | 5: 3.8–22.2 s | 2: 1.4–1.5 s |
| leader | 2: 1.8–1.9 s | 1: 2.4 s | 4: 1.5–2.4 s | 1: 1.4 s | 2: 1.4 s | 3: 3.6–6.3 s | 1: 3.1 s |
| quorum | 3: 12.8–15.1 s (+0.5 to +2.8 s) | 4: 38.9–39.1 s (-0.5 to -0.3 s) | 3: 31.0–31.9 s (+0.8 to +1.7 s) | 3: 35.6–39.7 s | 3: 13.0–13.8 s (+0.8 to +1.5 s) | 2: 52.5–53.1 s | 4: 45.2–49.5 s (-1.0 to -0.4 s) |

A session that lost nothing or a follower went on committing within half a second, except in k27
and m4, whose host held every sync for seconds at a time (the stalls, below); one that lost its
leader led again within 1.4 to 2.4 s (3.1 to 6.3 s in k27 and m4); one without its quorum wrote again
as its voters came back, from 1.0 s before to 2.8 s after the last of the twelve served again where
the table gives it (a voter of its own can serve before the last of the twelve does).

**k27's host.** k27 ran while the host's file system held every sync for seconds at a time: docker
took 11 s to kill the twelve and 31 s to start them (2 to 3 s and 5 to 6 s in the other kill runs),
so they were down some 61 s rather than 36 s; every session went without a commit for 4.8 s at once
78 s after the kill (the stalls, below); its lifecycles waited out their clocks for longer (1,523
`unavailable`: 1,265 `artifact.submit` and 87 `validation.report` in the sessions whose content copy
was down, 171 `claim.create` in the two without their quorum); and the directory partition, which
kept its quorum, healed three sessions under the load.

**Healing under the load.** The directory partition heals a session a death leaves a voter short
once the death has stood an election window (27 §3.1 P4), its leader committing the detector's
verdicts. In k27, whose twelve stayed down some 61 s, it healed three sessions: each dead copy's seat
went to a node outside the session and the surviving voter became a copy, the case 9d893dca closes,
its readiness verifying the session's artifacts and their objects (1,543 to 5,039, 54 KB to 4.4 MB)
in 0.2 to 2.5 s. It healed no session after the first without its quorum, in ledger order, though
four had lost one voter, and in m4, whose twelve were drawn outside the control plane, none: a
session the controller could not act on ended its pass (found and fixed below). m4's placement,
read every 2 s (`m4-0816/placement-samples.txt`), shows the directory recording the deaths one at a
time, the first 17.2 s after the kill and the twelfth at 40.7 s, every session missing a voter
`blocked` by its dead node from then, and no heal planned before the twelve served again at 48 to
50 s. On the fixed tree a heal reaches every session it can: m6 activated two sessions on new
voters while the twelve were still down and two more after (below).

**The stalls.** In six runs no session committed for a moment, all at once (k24 1.3 s, m2 2.1 s, c15
1.5 s, p9 1.1 s, k27 4.8 s, m4 1.4 s), while the load generator sent on time and each node's local
admin socket answered. They are the host's file system. Every node's log is on fedora's root btrfs
volume (`compress=zstd:1`, holding Docker's root), whose transaction commits hold every sync made on
it. In the two runs sampled every 200 ms (`k27-0749/host-stall.txt`, `m4-0816/host-stall.txt`), the
volume's NVMe completed no flush, and nearly no write, with nothing in flight at the device, from
77.8 to 82.0 s after the kill in k27 and from 9.6 to 10.8 s in m4; every session's commits stopped
over the same windows (78.0 to 82.6 s, 9.6 to 10.8 s) and came back with the first flushes, after a
burst of some 36,000 writes at up to 312 ms each in k27 and 21,000 at 58 ms in m4. In m4, whose
volume's commits were read every 200 ms as well (`m4-0816/btrfs-commits.txt`), one commit ran from
9.5 to 10.9 s after the kill, while docker killed the twelve, and took 1,467 ms; under the load
otherwise the volume committed every 20 to 30 s in 61 to 321 ms (m5: 8 commits in its 140 s of load,
none longer than 369 ms, its longest window without a commit 0.3 s), so the load alone does not make
them long. The volume had committed 5,327 transactions since the host's boot, the longest in 5.2 s.
Not the neighbour table (no forced collection and no table full since the boot, before all six),
nor name resolution (p9, k27 and m4 ran with c606af7b). The harness keeps all 33 nodes' logs on the
one volume, so its pause is every node's at once.

**Confirmation at 33 nodes (2026-11-01 and 02).** m6 and k28 ran on image `focal:fleet-c17`
(e558cefa, every fix above), after the host's kiln runner had ended every build and run it carried
from 10:20 to 17:09 under other tenants' tests and then went offline until 23:20. m7 and k29 ran on
`focal:fleet-c18` (b9f8a514, below), m8 and k30 on `focal:fleet-c19` (832a0567, below). Every run
used the final campaign's fault seed (1029), so k28, k29 and k30 killed the same twelve as k26 and
k27, the founder among them. The m runs' draw changed with the nodes holding control votes. Other
tenants shared the host throughout: kiln CI, another agent's builds and benchmark containers
pinned to CPUs 4 to 15 (beside m6 and k28's start), and a third agent's single lab node with a
device-mapper delay on its own loop device (beside m8 and k30).

| Run | Image | Fault | Written | Refused (by cause) | Unknown / abandoned | Write p50 / p99 (ms) | Moves activated (longest without a commit) |
|---|---|---|---|---|---|---|---|
| m6-2352 | fleet-c17 | twelve outside the control plane killed 60 s | 27,217 | 2,483 (`owner_bound` 22, `unavailable` 2,461) | 0 / 0 | 160.0 / 24,319.0 | 4 (5.2, 19.6, 8.1, 15.9 s), 1 in custody at the end |
| k28-0012 | fleet-c17 | twelve killed 30 s | 29,271 | 429 (`owner_bound` 33, `ingress_capacity` 49, `unavailable` 347) | 0 / 0 | 143.6 / 20,027.0 | 4 (8.7, 7.9, 12.9, 0.2 s), 1 in custody |
| m7-0110 | fleet-c18 | twelve outside the control plane killed, down to the run's end | 21,724 | 7,884 (`owner_bound` 20, `unavailable` 7,864) | 1 / 91 | 256.3 / 14,357.4 | 3 (7.5, 17.4, 15.9 s), 1 in custody |
| k29-0135 | fleet-c18 | twelve killed 30 s | 28,983 | 717 (`owner_bound` 28, `ingress_capacity` 40, `unavailable` 649) | 0 / 0 | 259.3 / 18,053.9 | 2 (15.6, 0.3 s), 1 in custody |
| m8-0415 | fleet-c19 | twelve outside the control plane killed 60 s | 27,494 | 2,206 (`owner_bound` 11, `unavailable` 2,195) | 0 / 0 | 134.5 / 20,746.0 | 3 (5.7, 8.4, 5.1 s) |
| k30-0434 | fleet-c19 | twelve killed 30 s (docker took 15.9 s to kill them) | 29,102 | 598 (`owner_bound` 234, `unavailable` 364) | 0 / 0 | 319.1 / 40,423.8 | none (the twelve back before a heal was planned) |
| k31-0454 | fleet-c19 | twelve killed 30 s | 28,970 | 730 (`owner_bound` 69, `unavailable` 661) | 0 / 0 | 173.3 / 22,235.1 | 2 (placement not sampled) |
| k32-0514 | fleet-c19 | twelve killed 30 s | 29,219 | 481 (`owner_bound` 34, `unavailable` 447) | 0 / 0 | 296.3 / 15,605.8 | 2 (placement not sampled) |
| k33-0529 | fleet-c19 | twelve killed 30 s | 29,575 | 125 (`owner_bound` 42, `unavailable` 83) | 0 / 0 | 180.3 / 15,470.0 | none |
| c18-0648 | fleet-c19 | twelve cut off 12 s, past the QUIC idle timeout | 29,664 | 36 (`owner_bound` 24, `ingress_capacity` 12) | 0 / 0 | 84.7 / 930.9 | 1 |
| p10-0707 | fleet-c19 | twelve partitioned 30 s | 29,610 | 90 (`owner_bound` 90) | 0 / 0 | 154.1 / 13,357.0 | none |

Each session's longest window without a commit in the run, by what the fault took from it (one
that moved after the fault has its move's window there):

| Run | Quorum | The leader | A follower | Nothing |
|---|---|---|---|---|
| m6-2352 | 3 sessions, 66.4 to 70.2 s | 1, 15.9 s | 4, 0.4 to 19.6 s | 3, 0.4 to 0.6 s |
| k28-0012 | 3, 37.4 to 41.2 s | 2, 1.5 to 2.7 s | 4, 0.6 to 12.9 s | 2, 0.5 to 0.7 s |
| k29-0135 | 3, 39.9 to 43.2 s | 2, 1.2 to 2.5 s | 3, 0.7 to 15.6 s | 3, 0.7 to 0.9 s |
| m8-0415 | 4, 65.6 to 70.6 s | none | 3, 0.8 to 5.7 s | 4, 0.2 to 0.9 s |
| k30-0434 | 4, 51.0 to 52.9 s | 2, 2.0 to 2.7 s | 2, 3.4 to 3.6 s | 3, 1.9 to 4.5 s |
| k31-0454 | 2, 41.1 to 42.0 s | 3, 1.6 to 2.4 s | 4, 0.4 to 10.8 s | 2, 0.4 to 0.6 s |
| k32-0514 | 3, 36.7 to 39.4 s | 3, 4.1 to 7.3 s | 3, 7.7 to 13.3 s | 2, 5.6 to 6.6 s |
| k33-0529 | 3, 38.3 to 40.6 s | 2, 1.5 to 2.4 s | 4, 0.4 to 0.5 s | 2, 0.4 to 0.5 s |
| c18-0648 | 3, 13.0 to 13.4 s | 3, 1.6 to 2.9 s | 3, 1.4 to 1.8 s | 2, 1.4 to 6.5 s |
| p10-0707 | 4, 31.0 to 31.4 s | 2, 1.3 to 1.4 s | 2, 0.3 to 0.5 s | 3, 0.2 to 0.5 s |

k30's write p99 carries the load generator's own lateness: its schedule delay reached a p99 of
7.5 s on a strained host (docker took 15.9 s to kill the twelve, against 1.4 to 3.0 s in the other
runs), and its open loop times a write from when it was scheduled. m8's longest window with no
commit in any ledger (15.9 s, 105 to 121 s after the kill) is one session's last steps. 79e8fb08
moved and was activated by 104.2 s, and its last 11 writes committed at 121.3 s, after every other
session had finished its work by 98 s. k32's sessions that lost nothing went 5.6 to 6.6 s without a
commit from 50.6 s, every session at once, as the twelve came back. All 33 nodes' shared-log I/O
threads were then blocked on disk: 1,350 of the 200 ms samples in those ten seconds, against 705
from 18 nodes in a quiet ten seconds before the kill. That is the volume's commits (condition 10).

What the runs confirm:

- **Heals reach every session they can (01a2f923).** In m6 the directory held the twelve dead
  from 22.6 to 36.8 s after the kill. Two sessions it could act on were activated on new voters
  while the twelve were still down (05debf56 by 52.3 s, 71c4614f by 59.0 s), two more after (by
  101.2 and 105.5 s), and a fifth was in custody at the end. In m5, on the tree before the fix,
  none was. k28, k29 and m7 moved four, two and three. A session most of whose voters were dead
  was passed over until they were back (quorum column).
- **No write's outcome stayed unknown and no lifecycle was abandoned** where the fault ended
  within the run (22132325, baabea7d): 0 and 0 in m6, k28 and k29. m7's 1 unknown and 91 abandoned
  are all in the four sessions that had no quorum to the run's end. Docker took 186 s to kill the
  twelve, with the host's disk saturated throughout (I/O pressure 50 to 76 % some, 17 to 64 %
  full), so they were started again only after the measured window. An agent abandons a lifecycle
  whose read went unanswered past a hand-off's wait (focal-load's `read_page`). These were in
  flight when the run stopped.
- **Every bootstrap completed** (c63c52c1, 6bb00314): m6, k28, m7 and k29 each enrolled 500
  agents. m7's bootstrap went through two root-group stalls (below) only because the bootstrap now
  asks an invitation again.
- **Every refusal is typed.** Each is the custody rule's (`unavailable` at the client's 30 s clock
  on artifact steps, `owner_bound` at the custody check), a session's without its quorum, or
  `ingress_capacity` (k28, k29 and c18: a session's door while its voters' syncs were held, below).

What the confirmation found, fixed at the cause:

- **A move's session was closed to its clients longer the later it came in ledger order**
  (b9f8a514). From its log's cutover until its activation, a moving session commits nothing (24 §3).
  A controller pass takes one step, the first session with one taking it, and sessions were served
  in ledger order. So a closed session waited through every step of a session before it that was
  still serving: m6's four windows were 5.2 s for the first in ledger order and up to 19.6 s for one
  whose three promotions waited behind another session's move. The controller now serves sessions
  nearest their activation first. m7's windows (7.5 to 17.4 s, on a host whose disk was saturated
  through the fault) show what is left. Each window opens at the log's cutover, and each existing
  voter re-verified its custody six to eight times (0.5 to 0.8 s each, about 4,000 artifacts and
  4.3 MB) before its readiness counted. The new copies' was taken the first time. The readiness
  intents went through the root's owner (next). With that fixed as well, m8's three windows were
  5.7, 8.4 and 5.1 s, and each copy verified its custody one to three times (two by design: once
  when custody is verified under the new route, again once the copy has applied the barrier).
- **The root group stalled while its owner synced node-local journals** (832a0567). Every node's
  placement intents (and its network controller's) were written by the root's control owner, which
  synced the journal's file and its directory itself. Sampled inside the nodes every 200 ms (k29),
  the founder's and focal3's root owners were in `fsync` on `placement-partition/.pending-…` and on
  the directory, in btrfs's `wait_log_commit` and in `barrier_all_devices`, a full commit. While it
  waits the root replica takes no tick, sends no heartbeat and steps no frame. In m7's bootstrap
  the founder's and focal3's root owners stalled together for 5.9 s and for 20 s, with no
  checkpoint under way and their floor at 170, and the group elected another leader as each stall
  ended. That is p9-0639's stop, which c63c52c1's retry got m7 past. Intents also queued behind the
  owner's other work and were refused `Capacity` when its queue was full. A journal writer of their
  own now makes them. On `focal:fleet-c19` (m8, k30), sampled the same way, the founder's root owner
  was in a sync in 3 of m8's 2,984 samples. Across k30's every node, control owners were blocked on
  disk in 97 samples, where the journal writers took 801. No root election came from an owner's
  sync. One 7.8 s stall of m8's root remained, and it was the volume's: the shared log's I/O thread
  sat in `fdatasync` in btrfs's `wait_current_trans` and `wait_for_commit` (a full transaction
  commit) while the root owner waited for its write (condition 10, below). It did not remove
  `ingress_capacity` (c18, below).

What the confirmation left, its cause since found:

- **`ingress_capacity` is a session's door full while its voters' syncs are held** (k28 49, k29 40,
  and c18 12 on the tree with 832a0567; none in k26, k27, m6, m7, m8, k30 to k33, p10 or the four
  runs with retrying agents; the earlier campaigns' c9 and c14, cut off as c18 was, had 5 and 15).
  Shown in c18, where every node's log I/O thread was sampled every 200 ms: from 11.7 to 13.0 s
  after the isolation began no session committed at all, the two that had lost nothing among them,
  and from 11.8 to 12.8 s the log I/O threads of the same sixteen live nodes were in uninterruptible
  disk waits in every sample: a stall of the one btrfs volume every node's log is on (condition 10,
  below). Ledger 6, which had lost a follower, is the one whose door filled: its two live voters'
  syncs had been held often from 10 s (their log I/O threads in D in most samples), its writes under
  way grew from 135 to 231, and once the volume stopped, the client's three capacity resends,
  throttled as gRPC throttles retries, were spent in 50 to 320 ms and its 12 claim creations
  reported `ingress_capacity`. k28's and k29's were each one session that had lost a voter, the
  others committing, so that its two live voters each held every commit: k28's ledger 5 committed
  nothing for 2.7 s from 16.0 s (an election its leader won again), k29's ledger 7 for 2.8 s from
  12.8 s. Those runs kept no thread samples, and a node's flush metrics keep no maximum (k28's
  focal2: p99 27 ms, p99.9 84 ms), so a voter's held sync is what fits them, not what they show. The
  door's refusal is its bound doing its work: what it refuses admitted nothing, and an agent that
  retries as the contract says commits it once the stall passes (above).

## Claims, artifacts, testaments and validations

The writes of every 33-node run, by what they are. The kinds are focal-load's work lifecycle
(`tools/load/src/driver.rs`): claims (its create, post, receipt acquire and release), artifacts
(submit), testaments (submit, post and receive), and validations (enter, begin and report). Each
write is counted once, as it ended. Every write that did not commit was refused with a typed cause
and its client told, except the two of unknown outcome shown. The first seven are the final
campaign (c606af7b, m4 with the harness at 902acac7). The rest are the confirmation, the last seven
on the tree with every fix (`focal:fleet-c19`, 832a0567, its tip changing only tests and
documents): a dozen killed (m8, k30 to k33), cut off for 12 s past the QUIC idle timeout so that
every connection they held dropped (c18), and partitioned for 30 s (p10). Each run's measure is
beside it (`kinds.txt`).

| Run | Fault (twelve of 33) | Claims committed | Artifacts committed | Testaments committed | Validations committed |
|---|---|---|---|---|---|
| c16-0602 | cut off 12 s | 106,394 of 106,394 (100.00 %) | 26,581 of 26,581 (100.00 %) | 80,115 of 80,115 (100.00 %) | 80,629 of 80,697 (99.92 %) |
| k26-0620 | killed 30 s | 106,235 of 106,237 (100.00 %) | 26,499 of 26,560 (99.77 %) | 79,794 of 79,794 (100.00 %) | 80,265 of 80,309 (99.95 %) |
| p9-0647 | partitioned 30 s | 105,786 of 105,786 (100.00 %) | 26,435 of 26,462 (99.90 %) | 79,425 of 79,425 (100.00 %) | 79,490 of 79,569 (99.90 %) |
| m3-0704 | killed, founder spared | 106,159 of 106,159 (100.00 %) | 26,500 of 26,531 (99.88 %) | 79,746 of 79,746 (100.00 %) | 80,145 of 80,180 (99.96 %) |
| c17-0721 | cut off 12 s | 105,780 of 105,780 (100.00 %) | 26,454 of 26,454 (100.00 %) | 79,458 of 79,458 (100.00 %) | 79,518 of 79,592 (99.91 %) |
| k27-0749 | killed, down 61 s | 104,250 of 104,422 (99.84 %), 1 unknown | 25,150 of 26,415 (95.21 %) | 75,829 of 75,829 (100.00 %) | 76,007 of 76,313 (99.60 %) |
| m4-0816 | killed outside the control plane | 104,007 of 104,272 (99.75 %) | 25,416 of 26,200 (97.01 %) | 76,375 of 76,375 (100.00 %) | 76,531 of 76,587 (99.93 %) |
| m6-2352 | outside control, 60 s | 100,777 of 101,793 (99.00 %) | 24,123 of 25,535 (94.47 %) | 72,627 of 72,631 (99.99 %) | 73,055 of 73,106 (99.93 %) |
| k28-0012 | killed 30 s | 105,288 of 105,338 (99.95 %) | 26,080 of 26,413 (98.74 %) | 78,340 of 78,340 (100.00 %) | 78,454 of 78,500 (99.94 %) |
| m7-0110 | outside control, to the end | 76,899 of 84,508 (91.00 %) | 18,971 of 19,194 (98.84 %) | 57,882 of 57,899 (99.97 %), 1 unknown | 59,263 of 59,299 (99.94 %) |
| k29-0135 | killed 30 s | 104,946 of 105,018 (99.93 %) | 25,787 of 26,391 (97.71 %) | 77,463 of 77,463 (100.00 %) | 77,585 of 77,626 (99.95 %) |
| m8-0415 | outside control, 60 s | 100,446 of 101,982 (98.49 %) | 24,675 of 25,278 (97.61 %) | 74,802 of 74,812 (99.99 %) | 75,699 of 75,756 (99.92 %) |
| k30-0434 | killed 30 s | 104,800 of 104,946 (99.86 %) | 26,131 of 26,305 (99.34 %) | 78,482 of 78,482 (100.00 %) | 78,146 of 78,424 (99.65 %) |
| k31-0454 | killed 30 s | 105,185 of 105,197 (99.99 %) | 25,846 of 26,463 (97.67 %) | 77,693 of 77,693 (100.00 %) | 77,782 of 77,883 (99.87 %) |
| k32-0514 | killed 30 s | 105,200 of 105,270 (99.93 %) | 26,027 of 26,392 (98.62 %) | 78,214 of 78,214 (100.00 %) | 78,352 of 78,398 (99.94 %) |
| k33-0529 | killed 30 s | 105,719 of 105,727 (99.99 %) | 26,376 of 26,450 (99.72 %) | 79,242 of 79,242 (100.00 %) | 79,359 of 79,402 (99.95 %) |
| c18-0648 | cut off 12 s | 105,782 of 105,794 (99.99 %) | 26,444 of 26,444 (100.00 %) | 79,417 of 79,417 (100.00 %) | 79,575 of 79,599 (99.97 %) |
| p10-0707 | partitioned 30 s | 106,461 of 106,461 (100.00 %) | 26,619 of 26,619 (100.00 %) | 80,249 of 80,249 (100.00 %) | 80,705 of 80,795 (99.89 %) |

A testament committed whenever the fault ended within the run. Claims went uncommitted only in
sessions without their quorum (refused `unavailable` at the client's 30 s clock), in k27's held
syncs, and in k28's, k29's and c18's `ingress_capacity` (below). Artifacts and validations went
uncommitted under the all-copies custody rule while a copy was down: an artifact refused
`unavailable` at the client's 30 s clock, and a validation's begin refused `owner_bound` at the
custody check. m7's twelve stayed down past its measured window.

## Every lifecycle completed: agents that retry as focal's contract says

The runs above ended a lifecycle at its first refused write, as focal-load then did: they measured
a client that reports a refusal and gives up. focal's agent contract says what an agent does
instead: "A `capacity` refusal admitted nothing; the reference stays `Pending` for a later retry",
and `request.retry` resends the exact frame (`skills/*/references/workflow-contract.md`). Every
refusal of those runs admitted nothing: a session's door full (`ingress_capacity`), no leader or
quorum within the client's 30 s clock (`unavailable`), or a required copy short of custody
(`owner_bound`, which the node refuses at eligibility, before the owner admits the frame).
905cafba gives focal-load `refused: retry` (run.sh `REFUSED=retry`): the same request, its
identity, bytes and generation, is sent again after the client's longest pause between resends
(500 ms) until its ledger commits it or a hand-off's wait (120 s) has passed since it was first
sent, and its first refusal is counted under `retried` by its cause (the report, and the write
CSV's `retried` column), so a run says what its agents met as well as what they completed. `end`
stays focal-load's default, the comparison's measure of an overload. Four runs on
`focal:fleet-c20` (905cafba, whose nodes are `focal:fleet-c19`'s: 832a0567, the commits since
changing only tests, documents and focal-load), the same seed (the same twelve, the founder among
them, for the kill, the cut and the partition; m9's twelve drawn outside the control plane), one
of each fault kind:

| Run | Fault (twelve of 33) | Written and judged, of 29,700 | Refused | Unknown | Abandoned | Writes sent again after a refusal (first refusal) | Write p50 / p99 / max (ms) |
|---|---|---|---|---|---|---|---|
| k34-1444 | killed 30 s, the founder among them | 29,700 | 0 | 0 | 0 | 166 (`unavailable` 149, `owner_bound` 17) | 36.0 / 18,985.9 / 38,523.7 |
| m9-1459 | outside the control plane, killed 60 s | 29,700 | 0 | 0 | 0 | 2,563 (`unavailable` 2,471, `owner_bound` 92) | 332.2 / 47,605.1 / 70,875.3 |
| c19-1544 | cut off 12 s, past the QUIC idle timeout: every connection dropped | 29,700 | 0 | 0 | 0 | 32 (`owner_bound`) | 40.3 / 1,661.5 / 15,726.5 |
| p11-1611 | partitioned 30 s | 29,700 | 0 | 0 | 0 | 27 (`owner_bound` 26, `unavailable` 1) | 37.1 / 9,443.1 / 37,180.4 |

By kind (`kinds.txt` beside each run), every write of every kind committed:

| Run | Claims | Artifacts | Testaments | Validations |
|---|---|---|---|---|
| k34-1444 | 105,930 of 105,930 | 26,483 of 26,483 | 79,559 of 79,559 | 79,723 of 79,723 |
| m9-1459 | 106,716 of 106,716 | 26,657 of 26,657 | 80,642 of 80,642 | 81,315 of 81,315 |
| c19-1544 | 105,891 of 105,891 | 26,459 of 26,459 | 79,504 of 79,504 | 79,730 of 79,730 |
| p11-1611 | 105,858 of 105,858 | 26,457 of 26,457 | 79,478 of 79,478 | 79,670 of 79,670 |

The writes sent again, by kind, and how long each took from its first send to its commit (median /
longest):

| Run | Claims | Artifacts | Testaments | Validations |
|---|---|---|---|---|
| k34-1444 | 53: 32.0 / 34.4 s | 92: 31.3 / 33.2 s | none | 21: 25.9 / 38.1 s |
| m9-1459 | 384: 46.4 / 67.1 s | 1,883: 50.6 / 70.8 s | 7: 67.0 / 67.2 s | 289: 63.9 / 70.9 s |
| c19-1544 | none | none | none | 32: 9.8 / 15.7 s |
| p11-1611 | none | 1: 31.6 s | none | 26: 24.2 / 36.0 s |

What a write waited for is what refused it first. In k34 the claims and artifacts were in the four
sessions without their quorum (first sent 2 to 8 s into the kill) and committed once the voters
served again (34.6 to 40.5 s), and the validations waited on a copy's custody. In m9 the twelve
were down for 66.4 s: an artifact waited for its copy's return (median 50.6 s), and in the four
sessions that had lost a content copy (2, 3, 5 and 9) the waiting artifact steps filled every one
of their callers' 1,024 places, so nothing more was sent in those sessions for 16.9 to 20.5 s (the
generator's schedule delay p99 12.4 s). c19's and p11's were validations refused at the custody
check while a copy was cut off. A write's latency now carries its wait (from its place on the
schedule to its commit), where the runs above counted committed writes only, so these p99s are not
comparable with theirs. No write ran past the 120 s wait.

Every window in which a session committed nothing while writes were under way (`stallio.txt`,
read against every node's log I/O thread sampled every 200 ms and the volume's btrfs commits, in
k34, m9 and c19; p11's samplers were cut by a restart of fedora's kiln runner) was a session
without its quorum until its voters were back, one that had lost its leader through its election
(1.3 to 2.5 s), one whose artifact steps waited on a copy (m9), or a move's cutover (m9, below). No
session stopped on the disk, and no write was refused `ingress_capacity`. The death verdicts named
only the faulted nodes in k34, m9 and c19.

So the all-copies custody rule decides how long an artifact waits while a copy is down, not whether
it completes: every artifact of the four committed under it. Acknowledging at a quorum of the
voters (f+1), each of which holds every artifact's object once it has applied the artifact, would
have committed m9's 1,883 without their down copies; the user decided it on 2026-11-03 (below).

## Every lifecycle completed: agents that send each write once

The runs above completed every lifecycle because focal-load's agents sent a refused write again, so
the agent carried the outage, in its turns and tokens. The user's decisions of 2026-11-03 say what
focal does instead. An artifact is acknowledged once every live content copy and at least a quorum
of the session's voters (f+1) hold it, a copy that is down catching up once it is back. focal itself
carries a write to its outcome, not the agent, and not by a longer clock: the client's fixed 30 s,
which nothing had measured or cited for a write, goes. Three commits make it so, and a fourth
corrects the third:

- 8cc84355: a client delivers what its ledger cannot take yet for a configured bound,
  `client.delivery_timeout_ms` (read by the CLI, the MCP adapter and code mode; doc 08). Unset, it is
  Kafka's producer default for the same bound, `delivery.timeout.ms` = 120,000 ms ("an upper bound on
  the time to report success or failure", ProducerConfig.java). Within the bound the client sends
  again every refusal that admitted nothing.
- 0388d9b2: the custody rule by kind of object (doc 26 §1). A session places f+1 content copies
  beside 2f+1 voters (two beside three), so a majority of its copies was every copy. A native
  artifact's bytes ride its frame, and a quorum of the voters holds them once it commits, so it needs
  every copy that is up and at least one (`CustodyRule::LiveCopies`), as does a phase that evaluates
  one. An object a frame names by reference needs a majority of its copies (`LiveMajority`), and an
  archive bundle every copy (`EveryCopy`). Custody short is refused `unavailable`, which the client
  sends again within its bound. A write whose outcome its client could not learn is sent again under
  its own identity within the bound, as Kafka's producer retries until `delivery.timeout.ms`.
- a5ef9aca: a heal whose cause has gone is withdrawn (doc 27 §5), and feba1336, which allows that only
  before any copy the heal brings begins to join the session's group (below).

Runs with focal-load's default `refused: end`: its agents send each write once, and the first
refusal their client reports ends the lifecycle. The same seed, faults and harness as above (the
report is `focal-220-128-work-agents500-nodes33-faults.json` again). "Sent again" counts the writes
whose client could not learn their outcome and which the agent sent again under their identity
(focal-load's `resent`):

| Run | Image (tree) | Fault (twelve of 33) | Written and judged, of 29,700 | Refused | Unknown | Abandoned | Sent again | Write p50 / p99 / max (ms) |
|---|---|---|---|---|---|---|---|---|
| k35-1423 | fleet-c21 (8cc84355) | killed 30 s, the founder among them | 29,676 | 24 (`owner_bound`) | 0 | 0 | 341 | 82.2 / 16,066.4 / 42,287.1 |
| m10-1436 | fleet-c21 (8cc84355) | outside the control plane, killed 60 s | 29,696 | 4 (`owner_bound`) | 0 | 0 | 0 | 281.0 / 48,649.2 / 77,296.2 |
| k36-1557 | fleet-c22 (a5ef9aca) | killed 30 s, the founder among them | 29,700 | 0 | 0 | 0 | 0 | 100.6 / 4,959.4 / 43,230.5 |
| m11-1643 | fleet-c22 (a5ef9aca) | outside the control plane, killed 60 s; the host's 15-minute load 1,040 as it began | 28,979 | 618 (`unavailable`) | 0 | 101 | 71 | 453.4 / 43,615.1 / 81,810.6 |
| m12-1724 | fleet-c22 (a5ef9aca) | outside the control plane, killed 60 s | 28,886 | 753 (`unavailable`) | 1 | 60 | 14 | 392.0 / 40,128.0 / 74,939.0 |
| c21-1751 | fleet-c22 (a5ef9aca) | cut off 12 s, every connection dropped | 29,700 | 0 | 0 | 0 | 0 | 60.3 / 511.9 / 14,894.2 |
| p13-1803 | fleet-c22 (a5ef9aca) | partitioned 30 s | 29,700 | 0 | 0 | 0 | 0 | 54.0 / 1,238.5 / 33,861.0 |
| m13-2254 | fleet-c23 (feba1336) | outside the control plane, killed 60 s | 29,700 | 0 | 0 | 0 | 0 | 52.3 / 7,216.5 / 72,206.0 |
| k37-2318 | fleet-c23 (feba1336) | killed 30 s, the founder among them | 29,700 | 0 | 0 | 0 | 0 | 39.1 / 14,960.6 / 41,788.7 |
| c23-2330 | fleet-c23 (feba1336) | cut off 12 s, every connection dropped | 29,700 | 0 | 0 | 0 | 0 | 31.1 / 216.4 / 15,205.6 |
| p14-2342 | fleet-c23 (feba1336) | partitioned 30 s | 29,700 | 0 | 0 | 0 | 0 | 51.3 / 831.5 / 36,140.4 |

By kind (`kinds.txt`):

| Run | Claims | Artifacts | Testaments | Validations |
|---|---|---|---|---|
| k35-1423 | 105,808 of 105,808 | 26,452 of 26,452 | 79,451 of 79,451 | 79,574 of 79,598 |
| m10-1436 | 105,846 of 105,846 | 26,453 of 26,453 | 79,466 of 79,466 | 79,651 of 79,655 |
| k36-1557 | 105,880 of 105,880 | 26,457 of 26,457 | 79,478 of 79,478 | 79,703 of 79,703 |
| m11-1643 | 104,429 of 105,010 | 26,114 of 26,120 | 78,974 of 78,990 | 79,556 of 79,571 |
| m12-1724 | 104,624 of 105,362 | 26,085 of 26,086 | 79,279 of 79,289 | 80,656 of 80,661 |
| c21-1751 | 105,844 of 105,844 | 26,453 of 26,453 | 79,460 of 79,460 | 79,638 of 79,638 |
| p13-1803 | 105,842 of 105,842 | 26,448 of 26,448 | 79,451 of 79,451 | 79,633 of 79,633 |
| m13-2254 | 105,880 of 105,880 | 26,455 of 26,455 | 79,495 of 79,495 | 79,722 of 79,722 |
| k37-2318 | 105,822 of 105,822 | 26,445 of 26,445 | 79,436 of 79,436 | 79,600 of 79,600 |
| c23-2330 | 105,816 of 105,816 | 26,446 of 26,446 | 79,428 of 79,428 | 79,594 of 79,594 |
| p14-2342 | 105,820 of 105,820 | 26,447 of 26,447 | 79,433 of 79,433 | 79,600 of 79,600 |

On `focal:fleet-c23` (feba1336) every lifecycle of every fault kind completed, with each write sent
once: every claim, artifact, testament and validation committed, nothing refused, unknown or
abandoned, and no write sent again by an agent. A write's latency carries its wait inside the
client, so these p99s are not comparable with the first runs'. The runs before found what feba1336
and 0388d9b2 close:

- **k35** (8cc84355): 24 validations refused `owner_bound`. Under bbcf7ad1's majority of copies, with
  two copies a majority was both, so a validation's begin needed the copy that was down. 341 writes
  sent again, each a write whose client could not learn its outcome and reported it at its attempts.
  0388d9b2 closes both: the copy that is up suffices (`LiveCopies`), and the client carries the
  uncertain write within its bound. m10 (the same tree) refused 4 the same way, and its artifacts all
  committed, each waiting inside its client for its copy's return.
- **m11 and m12** (a5ef9aca): one session in each closed to its clients until the run's end. m12's
  ledger 8 (6e2c7e70) lost a follower, focal21. Its heal was planned at 40.1 s, and by 42.5 s every copy
  had caught up; the controller promoted the new voter (focal18) and, once every desired voter voted,
  committed the session's cutover record. That record closes a session's serving until
  activation (`serves_route`). Every write started from 58.7 s on was refused, held by its client
  for its bound. The copies' readiness and promotion ran until 88.4 s. Meanwhile focal21 came back,
  the active placement verified again, and with no barrier in the directory yet (the directory
  learns of the cutover record only as the plan's barrier, later) the controller withdrew the heal
  between 88.4 and 90.7 s. No activation could follow a plan the directory no longer held, so the
  session stayed closed: 753 writes refused `unavailable` and one unknown, each after 119.5 to 240.5 s
  inside its client, 60 lifecycles abandoned, and nothing committed from 58.9 s to 325.7 s. m11's ledger 6
  (3f549a1c), on a host whose 15-minute load was 1,040 as the run began (103 other containers; its
  first attempt, m11-1611, ended without a report), went the same way: caught up by 51.8 s, refusing
  from 64.0 s, withdrawn between 80.7 and 83.4 s, closed from 64.6 s to the end (618
  refused, 101 abandoned). feba1336 withdraws a heal only while none of its copies has begun to join
  (`joining`): the log's configuration names none of them, and the directory records each as
  assigned, not yet installed. Before a copy joins no cutover can be under way, since it needs every
  desired voter voting in the log. m13 ran the same fault on that tree: no session stayed closed,
  and every write committed.

The report's "longest without a commit" and `stallio.txt` had measured a ledger's windows only
between two commits, so a session that committed nothing to the run's end showed none: m12's ledger
8 read 0.4 s. Both tools now count a window from a ledger's last commit to the end of a write still
unanswered after it (`~/s5/killreport.py`, `stallio.py`, "none committed after"). Every run's report
and `stallio.txt` here are made again with them; only m11's ledger 6 and m12's ledger 8 change.

The windows in which a session had writes under way and committed none, on the final tree
(`stallio.txt`): sessions without their quorum until their voters served again (35.1 to 37.4 s in
k37, 12.4 to 13.8 s in c23, 31.3 to 34.9 s in p14, 65.6 and 66.3 s in m13), elections after a lost
leader (1.3 to 2.5 s), and in m13 windows of 1.9 to 8.4 s in four sessions that lost a follower or
their leader (ledgers 2, 5, 7 and 10; the heals of 2, 7 and 10 activated, as their replaced voters'
copies show). m13 ran without its placement or disk sampled, so those windows are not tied to
cutovers. k37's ledger 9 had four windows of 1.1 to 1.4 s, 25 to 35 s
into the kill, and its ledger 10 one, each while 13 to 15 nodes' log I/O threads waited on the disk
under the kill's I/O pressure (23 to 24% some): not root-caused. No write was refused
`ingress_capacity`, and no session stayed closed.

One window was not a session stopping. m13's ledger 10 (3b7041d7) sent no write from 92.7 s to
190 s, then 1,105 in a burst at 190 to 201 s, every one committing within 0.1 s, across every step at
once. Its agents waited for their hand-offs, which each learns from its own inbox, read at its home:
one of the nodes that held the ledger's replica when the run began (focal-load's `Conn::open_homed`).
The heal moved the seat of focal31 (killed) to focal28, and focal31's copy was never released: at the
run's end focal31 still had it installed and running, frozen at index 26,908 of the group's 29,944
and knowing no leader. A replica serves an inbox read while its own last configuration names it a
voter (`serves_all_members`), and a copy that leaves its group while it is down never applies the
change that removed it. So the agents homed on focal31 read their inbox from a copy that took no
more of the log. When it stopped answering them is not shown (m13 ran without its samplers); that
the frozen copy held them is the likely way. Every hand-off arrived within its 120 s wait: none was
undelivered, and every lifecycle completed. No node releases a copy its session no longer places on
it. `ReplicaFleet::remove` exists and nothing calls it, a restarted node reopens every copy it ever
installed (`reopen_installed`), and the record of a node's copies is capped at 1,024 for its life
(`MAX_INSTALLED`). Every run in which a heal activated ends with such copies, the replaced voters'
(m9 three, m10 three, m12 two, m13 three), and no other run has any. This is open (below).

## Found and fixed at the cause

What the kill and partition campaigns left open, and what these runs found, each fixed at its cause
(5a7af464 to 6bb00314), with the test that fails without its fix:

- **An artifact job asked a copy the liveness view held dead** (the evidence door's `ingress_capacity`
  refusals). A new leader holds none of its predecessor's receipts, so on every evaluating phase it
  asked the dead copy, holding a driver slot for the pool's two 5 s waits, and the door let arriving
  jobs pass the ones it held. An obligation now asks no copy the view holds dead and waits on none it
  comes to hold so, and the door is first come first served (015a5835).
- **Failover waited on the QUIC idle timeout** (10 s while a killed leader's successor led within
  2.6 s). A client now sends a request unanswered for its retransmission timeout (RFC 6298 over the
  answers it is given) by the next route as well, the first answer that admits something winning
  (c7bcc866).
- **The directory partition's authority refresh carried the whole enrollment registry** in one
  128 KiB command (about 500 enrolled clients). From level 12 a partition is given the nodes alone
  (1d4dc1d7).
- **focal-load held a run to 64 callers** (5 a ledger over 11 sessions; f8d1be7f).
- **A node's local intent journal was let go after 5 s of the wall's clock**: a slow disk stopped the
  founder's root network owner in two bootstraps and twelve nodes' placement intents in a third. A
  write admitted is waited for to its answer (67e7900b); an owner that stops says why (07630dd8).
- **A session refused every write until the run ended** (c6, 2026-10-30: 1,003 writes and 79
  lifecycles; k22 and m1, 2026-10-31: 179 and 292 writes, 79 and 61 lifecycles). In each a move
  replaced the session's one voter that held no content copy (the voter killed or cut off) and
  stopped where the new voter's readiness was due, the session closed to clients from the move's
  cutover until activation. A readiness verifies the session's every artifact, and it read every
  object the session's evidence names from the node's own store whatever the node's role, a seal's
  bundle among them; but the copies required to hold the objects are the placement's content copies
  (26 §1), and the archive gives a seal's bundle to them alone. The new voter, brought up by a
  checkpoint, held every artifact's object (an install pulls each it lacks) and no bundle (m1's had
  applied every record, nothing pending, no seal applied against its copies' one), so its store
  answered not found, which the content host reports as "outcome unknown" (the error its agent showed
  in c6, k17, k22 and m1, the content owner answering calls throughout, its threads read through
  `/proc` in m1), on every attempt. A node now verifies the objects' bytes only where it is a
  content copy; every member verifies every artifact and the object it names (29037764). And a voter
  a move makes a copy, which is how a heal replaces a dead copy (the copy that sits stays one, the
  surviving voter becomes the other), holds none of the bundles: its verification now hands each
  object it lacks to the agent, which brings it from the active placement's copies, and reads the
  artifact again (9d893dca). k27 healed three sessions under the load this way (below), each
  surviving voter made a copy verifying every object its session's evidence names.
- **The directory partition refused 85% of the intents sent it** (c7, 2026-10-31: 7,576 of 8,868).
  Every partition command compared the whole partition's revision, which every commit moves (each
  node's load report, each liveness verdict, every session's change), and the authority's and
  enrollment's revisions, which every client's enrollment moves, while an agent's intent is journaled
  one pass and sent the next: with the partition taking a commit every second or so, an operation
  lost those compares to whatever moved in between, each loss a journal write, a round trip and a
  replan, and a readiness, prepared a whole verification before it was sent, the most often. From
  level 13 a node's enrollment, a load report, a liveness verdict and a session's change are compared
  against what each changes alone (its node's record, last report or verdict, its session's revision
  for a decision, nothing for a copy's report), each still validated as before, and their evidence by
  its proofs against the authority as it stands (8d40e22f): the next runs refused 36 to 102 of about
  1,250 to 1,575. A failed agent pass now names its step and session, refusals are counted by kind
  and failure, and a content call answered after its caller gave up is logged (0e7b457a).
- **Writes of a session led again within 1.6 s were refused `unavailable` at their clocks** (k21,
  2026-10-31: 31, begun from 2 s before the kill to 8 s after it by 13 agents). Each request whose
  timer expired doubled its client's one retransmission timeout, so a client with several requests
  waiting on the killed leader took it past their 30 s clock, and the requests it sent next sent
  themselves by no other route and waited on the dead one alone. The timeout now backs off once for
  each timeout that expires, however many requests waited on it, as a sender's one timer does (RFC
  6298 §5.5; 9f7925b4).
- **Every session's commits waited out each checkpoint wave** (c10 to c13, 2026-10-31/11-01): about
  1 s a wave in c10 to c12, 2 to 3.6 s in p6, and in c13 5 to 7 s in 9 of the 11 sessions at once,
  33 s after the heal, the sampler, which asks the nodes, taking no sample for 5.5 s. Sessions that
  began together and apply at one rate reached the checkpoint floor (4,096 entries) at one index:
  all 33 replicas checkpointed at indices 18,520 to 18,870 within seconds, their seeds' syncs meeting
  on the volume the nodes share (the slowest checkpoint commit 6.2 s). Each replica now checkpoints
  at its own point of the floor's period, drawn from its session and node (Floyd and Jacobson 1994;
  the floor gates the size rule, so a checkpoint only comes sooner; 5857518d).
- **A client resolved its route's name at every call** (found tracing the all-session stalls below,
  2026-11-01): `RouteConnections::request` resolved the route's name before each exchange though its
  connector held the route's connection, so the load generator resolved its endpoints thousands of
  times a second through the process's four places for resolution, the share sized for a node's
  round of its peers' names, and any lookup held up would have held every request of every session
  behind it: a killed node's name took 3.0 to 7.2 s to fail to resolve while it was down, a live
  one's under 10 ms (2,220 lookups timed from inside the generator's container through m3). A
  route's name is now resolved when the route is dialed, and again only once its connection or a
  dial to its address failed (c606af7b). It was not the stalls' cause, which went on with resolution
  once per dial (p9, k27): they were the host's file system (below).
- **An agent gave up a lifecycle a lagging replica could not yet read** (m3, 2026-11-01): one read
  of a lifecycle's state came back "serving replica is behind" from a node of the session started
  again and still catching up, and focal-load's agent, which asked again only after no answer or
  `Unavailable`, abandoned the lifecycle (k27's three abandoned likewise). A read a replica behind
  its token answers is now asked again like one no node could answer, within a hand-off's wait
  (`read_again`; baabea7d, after the final runs: it changes the generator alone);
  `a_read_a_replica_behind_answers_is_asked_again` fails without it.
- **A node whose route was older than its client's sent the client back to it** (k27, 2026-11-01:
  the one write of the final runs whose outcome was unknown, `client: invalid server response`, in
  a session healed under the load, its route epoch moved from 2 to 3, sent while the twelve nodes
  were starting again). A node holding no open replica of a ledger points a client at the ledger's
  leader as its route cache resolves it, and it handed over the cache's route whatever its epoch,
  where a node holding a replica takes the newer of its route's epoch and the client's. A client
  that has learned a later epoch adopts no older one and takes such an answer for an invalid one,
  which for a write is the only way to that error, so it ended the write unknown. A node whose
  route is older than the client's now answers it unavailable, and the client asks another route
  (22132325); `a_node_whose_route_is_older_than_the_clients_points_it_nowhere` fails without it.
- **A session the controller could not act on stopped the control of every session after it** (k27,
  m4 and m5, 2026-11-01). The directory partition's leader controls the sessions in ledger order,
  and its pass ended at the first whose control failed: one most of whose voters were dead, which
  nothing asked of it could commit, or one whose leader was dead, each call waiting out the 5 s
  session-control clock. So in k27 the three sessions healed all came before the first without its
  quorum, and none after it was, though four of them had lost one voter; m4 healed none, its first
  session in ledger order having lost two of its three voters; and m5, its twelve down 66 s, none in
  the 26.6 s after the directory held every death (`m5-0838/placement-samples.txt`). A session most
  of whose voters the directory holds dead is now passed over until enough are back, a leader it
  holds dead is not asked (a voter of the session it holds alive is, and answers where the log
  leads), and a session that fails leaves the sessions after it their turn (01a2f923);
  `a_session_can_change_its_voters_only_while_most_of_them_live`,
  `a_session_the_controller_cannot_act_on_leaves_the_others_their_turn`. Its run at 33 nodes waits
  on fedora (below).
- **The run that spares the founder did not keep the directory partition placing** (m3): the
  partition's two voters beside the founder are drawn at each bootstrap (m1: focal3 and focal23;
  m2: focal2 and focal15; m3: focal2 and focal6), and m3's twelve took both, so the partition placed
  nothing while they were down and no session moved. A fault's count can be drawn from the nodes
  holding no vote in the root's group or a partition's as the fault begins (`kill=12-control`;
  902acac7), and m4 ran so.
- **A client's enrollment asked once** (c14's first attempt, 2026-11-01): after all 33 nodes had
  joined and the 11 sessions had activated, one of the generator's clients answered "enrollment
  outcome is unknown; retry the saved join identity" and the bootstrap stopped, its host's load
  rising from 14 past 100. A node's join has asked again, the same join exactly, while the
  sponsor's answer says to and the invitation lives, since 2026-10-20; a client's asked once. Both
  now ask through one loop (`network_join::settle`; 09d5d1eb).
- **A join whose sponsor's name did not resolve gave up as if its invitation were invalid** (m6's
  second attempt, 2026-11-01: the 426th agent's enrollment answered "invitation or saved join state
  is invalid" while the host ran at a load near 100 with 2 GB of 62.5 free, and the bootstrap
  stopped). A node's join and a client's enrollment both answered every resolution of the sponsor's
  name that failed, in its 5 s or at all, as an invalid invitation, which no join asks again. The
  same enrollment, asked again later in the container the run left, enrolled in 1.3 s, so its saved
  state was sound, and the sponsor answers no such failure: of the checks that answer it, the name's
  resolution is the one that depends on the moment. A name that did not resolve is now the sponsor
  unavailable, which a join asks again until its invitation expires, and an endpoint that is neither
  an address nor a name stays invalid (6bb00314);
  `a_sponsor_whose_name_does_not_resolve_is_asked_again_and_a_malformed_one_is_not`.
- **A restarted follower's snapshot install waited on a seed chunk** (once in the kill campaign's
  eleven runs, a build before its restore-ceiling fix): recorded since 9a2c7680 (`seed_refused`); in
  every run since, no copy refused, lacked a seed chunk or lacked an object.

## The runs that found what this one closes

Each on fedora with the same load and the same twelve nodes, on the tree its row names (each run's
`tree.txt`), the image built from it:

| fedora run | Tree | Fault | Written, of 29,700 | Refused (cause) | Unknown / abandoned / undelivered | Write p50 / p99 (ms) | What it showed |
|---|---|---|---|---|---|---|---|
| k16-1505 | 1d4dc1d7 | killed 30 s | 29,646 | 54 (`owner_bound` 36, `unavailable` 18) | 0 / 0 / 0 | 124.8 / 14,660.5 | — |
| k17-1540 | 1d4dc1d7 | killed 30 s | 28,766 | 875 (`owner_bound` 40, `unavailable` 835) | 0 / 49 / 10 | 117.0 / 17,793.2 | a session without its quorum wrote again 53.7 s after its voters restarted (the others 15.7 to 46 s), losing 49 lifecycles and 10 hand-offs; a placement agent's pass ended on a content call "outcome unknown" |
| k18-2135 | 07630dd8 | killed 30 s | 29,300 | 400 (`owner_bound` 78, `unavailable` 322) | 0 / 0 / 0 | 128.2 / 19,387.4 | — |
| k20-2237 | 67e7900b | killed 30 s | 28,591 | 1,109 (`owner_bound` 104, `unavailable` 1,005) | 0 / 0 / 0 | 136.5 / 18,740.5 | — |
| p6-2256 | 67e7900b | cut off 30 s | 29,647 | 53 (`owner_bound` 29, `unavailable` 24) | 0 / 0 / 0 | 160.8 / 14,896.6 | — |
| c6-2316 | 67e7900b | cut off 12 s | 28,506 | 1,110 (`owner_bound` 107, `unavailable` 1,003) | 0 / 79 / 5 | 128.1 / 1,191.1 | a move stuck at its cutover: the session closed to every write to the end |
| c7-1320 | 78512b58 | cut off 12 s | 29,580 | 120 (`owner_bound` 120) | 0 / 0 / 0 | 114.4 / 1,203.8 | 7,576 of 8,868 partition intents refused at the partition's compare |
| c9-1449 | 6bbe0636 | cut off 12 s | 29,661 | 36 (`owner_bound` 31, `ingress_capacity` 5) | 0 / 3 / 0 | 131.8 / 2,531.9 | 1,335 of 2,536 refused, at the evidence's compare while clients enrolled |
| c10-1617 | 8d40e22f | cut off 12 s | 29,549 | 151 (`owner_bound` 151) | 0 / 0 / 0 | 186.5 / 1,652.7 | 77 of 1,575 partition intents refused |
| k21-1649 | 8d40e22f | killed 30 s | 29,546 | 154 (`owner_bound` 56, `unavailable` 98) | 0 / 0 / 0 | 159.7 / 17,822.4 | 31 writes of a session led again within 1.6 s refused `unavailable` at their clocks: the clients' retransmission timeout backed off once per waiting request |
| p7-1712 | 8d40e22f | cut off 30 s | 29,330 | 370 (`owner_bound` 320, `unavailable` 50) | 0 / 0 / 0 | 648.1 / 17,995.9 | — (the load generator itself delayed, its schedule p99 349 ms) |
| c11-1806 | 9f7925b4 | cut off 12 s | 29,541 | 159 (`owner_bound` 159) | 0 / 0 / 0 | 196.4 / 5,347.9 | — |
| k22-1837 | 9f7925b4 | killed 30 s | 28,851 | 763 (`owner_bound` 62, `unavailable` 701) | 0 / 79 / 7 | 184.8 / 17,540.4 | a move stuck at its new voter's readiness, its session closed from the cutover to the end |
| m1-1858 | 9f7925b4 | a dozen killed, the founder spared | 28,540 | 1,096 (`owner_bound` 137, `unavailable` 959) | 0 / 61 / 3 | 210.9 / 17,617.3 | likewise; the new voter's content owner, its threads read live, answering every call |
| c12-0129 | 6f3277df | cut off 12 s | 29,456 | 244 (`owner_bound` 244) | 0 / 0 / 0 | 391.3 / 6,993.8 | every refusal a `validation.begin` in the two sessions whose leader was cut off (215 and 29) |
| c13-0133 | 9d893dca | cut off 12 s | 29,635 | 65 (`owner_bound` 65) | 0 / 0 / 0 | 132.3 / 3,658.3 | 59 of the refusals in the two sessions whose leader was cut off; 9 sessions without a commit for 5 to 7 s at one checkpoint wave |
| c14-0321 | 5857518d | cut off 12 s | 29,573 | 127 (`owner_bound` 112, `ingress_capacity` 15) | 0 / 0 / 0 | 157.3 / 1,465.9 | no checkpoint wave: the longest window without a commit in any session 0.3 s |
| k24-0421 | 09d5d1eb | killed 30 s | 29,598 | 102 (`owner_bound` 21, `unavailable` 81) | 0 / 0 / 0 | 249.1 / 22,394.5 | every session without a commit for 1.3 s at once, 71 s after the kill (the stalls, below) |
| p8-0439 | 09d5d1eb | cut off 30 s | 29,687 | 13 (`owner_bound` 12, `unavailable` 1) | 0 / 0 / 0 | 91.9 / 4,525.6 | — |
| m2-0457 | 09d5d1eb | a dozen killed, the founder spared | 29,551 | 149 (`owner_bound` 50, `unavailable` 99) | 0 / 0 / 0 | 119.1 / 14,929.4 | the partition kept its quorum and no session moved, the twelve serving again 38.6 to 40.7 s after the kill; every session without a commit for 2.1 s at once |
| c15-0515 | 09d5d1eb | cut off 12 s | 29,639 | 61 (`owner_bound` 61) | 0 / 0 / 0 | 89.2 / 1,277.5 | every session without a commit for 1.5 s at once, 10.4 s into the cut |
| k25-0531 | 09d5d1eb | killed 30 s | 29,078 | 622 (`owner_bound` 103, `unavailable` 519) | 0 / 0 / 0 | 113.5 / 19,288.9 | the sessions with a content copy killed held their artifact steps to the clients' clocks (416 `artifact.submit` refused `unavailable`, the custody rule's) |
| m5-0838 | c606af7b (harness 902acac7) | twelve outside the control plane killed 60 s | 25,275 judged (25,269 written) | 4,431 (`owner_bound` 75, `unavailable` 4,356) | 0 / 0 / 0 | 179.9 / 22,153.0 | no session healed in the 26.6 s between the directory holding the twelfth death (42.1 s) and the twelve serving again; the detector settled the deaths 17.9 to 36.2 s after the kill (a suspicion runs 4.6 to 27.6 s at 33 members, the shorter the more members confirm it) |

Five bootstraps stopped before their load: c14's and k24's first (5857518d), each at a client's
enrollment that asked once (09d5d1eb); p9's first (c606af7b) at an admin call the founder could not
answer (Open); m6's second (c63c52c1) at the 426th agent's enrollment, answered "invitation or saved
join state is invalid" while the host ran at a load near 100 with 2 GB of its 62.5 free (a sponsor
whose name did not resolve was answered as an invalid invitation; 6bb00314, below); and m6's first
(01a2f923), at an agent's invitation answered unknown while the host's
file system held every sync for 5 s at a time (its volume's commits took 5.4 and 4.9 s in the half
minute before, `m6-0918-bootstrap/bootstrap-stop.txt`), the cluster founded and its sessions active.
An invitation is exact by its name and the CLI says to recover it, so the bootstrap now asks one
again as it does its other steps (c63c52c1).

Every `unavailable` refusal in k16 to p6 is one of two kinds, each read from the report's steps: an
artifact step (`artifact.submit`, `validation.report`) held at the client's 30 s clock while a
content copy of its session was down (the all-copies custody rule), and a session's first write
(`claim.create`) while its quorum was down. Every `owner_bound` refusal is a `validation.begin`
refused by the custody check ("N of M required copies hold the artifact"). k17's and c6's
`unavailable` beside those, and c6's, are a session's whole rate while it could not write: k17's
session without its quorum until 53.7 s after its voters restarted (on a tree before the local
intent journal's fix, 67e7900b; the same fault on every later tree lost no lifecycle), c6's from its
move's cutover to the end of the run.

In the 12 s cuts from c7 on, the refusals are custody checks: `owner_bound` at `validation.begin`
(all of c7's 120, c10's 151, c11's 159 and c12's 244; 31 of c9's 36, beside 5 `ingress_capacity` at
the evidence door), begun during the cut and answered at once or within the cut (the slowest at 8.5 to 16.8 s). Nearly all fall in the
sessions whose leader was cut off (c7: 112 of 120; c10: 143 of 151; c11 and c12: all), the rest in
sessions without their quorum. The new leader led at once (c12: within 1.3 s), but custody receipts are each
leader's own: a new leader holds none of its predecessor's, so a phase that evaluates an artifact
asks every required copy again (doc 04 §7, R8 instruction 1), and the copy cut off, the old leader,
cannot answer until the heal. c1 and c2 (5a7af464) refused none because an obligation then asked the
cut-off copy and waited out the pool's two 5 s attempts, by which time a 12 s cut had healed; that
wait held the evidence driver's places, and in the kill campaign 271 and 218 jobs ran out of time
behind it (015a5835).

## Open

- **A copy its session no longer places stays on its node, and serves.** No node releases a copy
  once its session's directory has retired it: `ReplicaFleet::remove` exists and nothing calls it,
  a restarted node reopens every copy it ever installed (`reopen_installed`), and the record of a
  node's copies is capped at 1,024 for the node's life (`MAX_INSTALLED`), so a node a thousand
  placements have passed through refuses every new copy. Known since 2026-10-11 (read, not
  measured); every run in which a heal activated ends with the replaced voters' copies installed
  (m9, m10, m12 and m13). A copy that left its group while it was down never applies the change that
  removed it, and serves an inbox read while its own last configuration names it a voter
  (`serves_all_members`), so agents homed on its node read from a copy that takes no more of the
  log: m13's ledger 10 sent no write for 97 s, its hand-offs waiting, then completed every
  lifecycle (above). The fix is the copy's release at its retirement (the replica stopped and
  removed, its record and its maps' keys with it, its log's member dropped) and a replica that
  serves only while the directory's placement names it.
- **A session without its quorum takes no write** until its voters are back: killed, until they serve
  again after their restart; cut off, until the heal. Its writes begun meanwhile wait inside their
  clients, within the delivery bound (`client.delivery_timeout_ms`, 120 s unset), and commit once the
  voters serve again (k37, c23, p14 and m13, above); a session without its quorum for longer than the
  bound reports its writes refused.
- **A move closes its session to clients** from its log's cutover until its activation (24 §3).
  Measured with the placement read every 2 s, the windows were 5.2 to 19.6 s in m6 and 7.5 to 17.4 s
  in m7. Part of that was a closed session waiting behind other sessions' steps (fixed, b9f8a514).
  The rest is each existing voter's custody, verified again on every pass until its readiness was
  taken (the confirmation, above), and a window still holds the custody verification and the
  directory's steps from promotion to activation. With 832a0567 the windows were 5.1 to 8.4 s (m8),
  and a moved session's last steps can still finish well after its activation: m8's 79e8fb08
  committed its last 11 writes 17 s after it.
- **A node never cut off was held dead.** In c18 focal33, never cut off, was held dead (seen at
  40.2 s, the sampler having read the cut-off founder until then) and alive again 2.2 s later, and
  96bc77a5's move off it ran to its activation: 6.5 s without a commit (66.0 to 72.5 s) in a session
  that had lost nothing. A gossiped death at a member's own incarnation is taken at once (SWIM's
  override, `liveness/driver.rs` `on_update`), and the twelve rejoined from a cut in which they had
  held the others suspect, so a stale verdict reaching the partition's leader before the accused's
  refutation is the likely way; not shown, since no other run with its placement sampled held dead a
  node its fault had not hit. A heal whose cause has gone is now withdrawn while none of its copies
  has begun to join (a5ef9aca, feba1336; doc 27 §5); once one has, the move completes, costing its
  session a move's window, never a write.
- **A readiness verifies a session within one export** (10 s): the copies of the sessions k27 healed
  verified 1,543 to 5,039 artifacts and their objects (54 KB to 4.4 MB) in 0.2 to 2.5 s on this
  host, so a session some ten times larger than these runs leave would need its verification
  carried across exports, which is not done.
- **Creating a session and the structural operations** (sealing, releasing, absorbing and installing a
  partition) still compare the whole partition and its authority: each happens once per session or
  partition change.
- **A node's log syncs wait on its file system's commits.** On this host's btrfs volume a commit held
  every sync for up to 5.2 s, and with every node's log on the one volume, every session stopped at
  once (the stalls, above). The long commits came with the host's own work (m4's while docker
  killed the twelve), not with the load, under which the volume committed in 61 to 369 ms. Whether
  focal should keep its logs out of the volume's copy-on-write on btrfs (`chattr +C` on its data
  directory), and what that changes in such commits, is not measured (condition 10, medium-aware
  disk).
- **A bootstrap stopped at an admin call the founder could not answer** (p9's first attempt,
  p9-0639, once in the 69 runs of the three campaigns; root-caused by the confirmation, above: the
  root's owner syncing node-local journals, fixed in 832a0567 and confirmed in m8 and k30): the first agent's invitation, asked of the
  founder just after the bench's and the consumer's had committed and both clients had enrolled,
  ran out the admin socket's 20 s (what follows is the account as these runs left it). Through those 20 s the founder led the root group at term 1 and
  committed nothing. The group elected focal2 at term 2 only as the call ended: its first entry
  (612) committed while the nodes' health was read, 1.5 to 9.5 s after the call ended: focal2, read 1.7 s
  after it, had applied 611, and focal4, read 0.6 s later, and every node after it, 612. The founder, at term 1,
  named no leader: it had left its leadership without hearing a later term, as a quorum check
  leaves a leader. focal3 still named the founder, at 611. So for some 20 s the founder's root owner
  took no step that committed: its own write or checkpoint held (its replica takes no input while
  one is in flight, `ControlHost::must_wait`, and the owner's thread waits for it), or the owner
  starved of CPU or memory in its container. Not root-caused: the health says none of this, and the
  host's kernel journal for the window has no neighbour-table overflow, OOM kill or hung task. A
  failed bootstrap now keeps each node's metrics (`focal_root_period_longest_ms`, the longest period
  its root owner took; `focal_root_periods_refused_total`), its container's CPU, disk and memory
  pressure and memory events, and the host's pressure and its volume's commits, so the next stop
  shows which one.

## The records

Beside this record, a directory a run (`2026-10-31-fleet33-condition3/<run>/`): its report
(`focal-220-128-work-agents500-nodes33-faults.json`), its placement at the end and by session, its
faults, the report by ledger (`killreport.md`), the agents' state (`agents.md`), the sampler of the
sessions whose leader was hit (`sessions-sampled.txt`), the host before and after (`host.txt`), the
run's status, tree and fit, the windows without a commit (`gaps.txt`), and each file's SHA-256 as
fedora computed it (`sha256.txt`, checked here after the copy); k27's and m4's host at their stalls
(`host-stall.txt`), m4's volume commits (`btrfs-commits.txt`), m3's lookups of the nodes' names
timed from inside the generator's container (`dnsprobe.log`), m4's, m5's and the confirmation runs'
placement every 2 s through the fault (`placement-samples.txt`), with the founder's detector counts
beside it from m5 on (`placement-metrics.txt`). The confirmation runs keep each moved session's longest window
without a commit around its activation (`moves-windows.txt`). m7 and k29 also keep the founder's and
focal3's root replica every second from their bootstraps (`root-samples.txt`: the floor, applied
index, term, leader, periods and longest period). k29 keeps, sampled inside the founder's and
focal3's containers every 200 ms, the root owner's, the hyper-log thread's and the hyper-log I/O
thread's state, syscall and wait channel, and the file the owner was syncing
(`syscalls-focal1.txt`, `syscalls-focal3.txt`). m8 keeps the founder's the same way, and k30
keeps, per thread name, how many 200 ms samples of every node found a thread blocked on disk
(`blocked-threads-summary.txt`), as k31 to k33 do. The four runs with retrying agents name it in their report
(`focal-220-128-work-agents500-nodes33-retry-faults.json`) and keep each kind's writes and what was
sent again (`kinds.txt`), every window without a commit read against the disk (`stallio.txt`), and,
for k34, m9 and c19, the volume's btrfs commits and the host's I/O pressure every 200 ms
(`volume-samples.txt`) and how many nodes' log I/O threads were blocked on disk at each 200 ms
sample (`blocked-threads.txt`); p11's samplers were cut by a restart of fedora's kiln runner. c18
keeps the same count (`blocked-threads.txt`), its volume stall in it: sixteen nodes' log I/O
threads in disk waits at every sample from 11.8 s after the isolation began, twenty-three by 12.8 s.
The runs whose agents send each write once (k35 to p14) keep the same files, their report under
its first name (`focal-220-128-work-agents500-nodes33-faults.json`), with `stallio.txt` and the
report by ledger made again with the window from a ledger's last commit to the end of a write
still unanswered after it, and the windows by cause (`gaps.txt`); m13 ran without its samplers
(only `stallio.txt`, `gaps.txt` and `kinds.txt` beside its report), and c21 without its placement
sampled.
Each run's whole directory, every node's log,
health, metrics and replicas among them, is on fedora under `~/s5/runs/<run>` (the intermediate
runs' too), beside the run kit (`~/s5/campaign.sh`, `fleet-run.sh`, `killreport.py`, `agents.py`,
`gaps.py`, `hostsample.sh`, `placesample.sh`, `btrfssample.sh`, `stallzoom.py`, and for the
retrying runs `samplers.sh`, `dsample.py`, `iosample.py`, `dsum.py`, `stallio.py` and `kinds.py`;
`killreport.py` and `stallio.py` as before the window to a ledger's end beside them, `.pre-tail`),
with every node's per-thread samples (`~/s5/dsample-<run>.txt`, c18's among them).
