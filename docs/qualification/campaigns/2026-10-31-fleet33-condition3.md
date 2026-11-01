<!-- Qualification record: goal condition 3 at scale, a dozen of 33 focal nodes killed, partitioned and cut off under the work flow, 2026-10-31 to 2026-11-01, fedora (Linux x86-64), the final runs at fleet-run c606af7b (m4 with the harness at 902acac7), every fix the runs found at 6bb00314, its confirmation at 33 nodes pending. -->

# Condition 3 at scale: a dozen of 33 nodes killed, partitioned and cut off under the work flow

Dates: 2026-10-31 to 2026-11-01. The final runs at `fleet-run` c606af7b (image `focal:fleet-c15`),
the tree on which the defects that [the kill campaign](2026-10-29-fleet33-kill12.md), [the
partition campaign](2026-10-30-fleet33-partition12.md) and the runs below found close; m4 ran the
same image with the harness at 902acac7, which draws a fault's nodes from those outside the control
plane (below). What the final runs found closes at five commits: 01a2f923 (a session the controller
could not act on stopped the control of every session after it), 22132325 (a node whose route was
older than its client's sent the client back to it), baabea7d (focal-load: an agent asks again a
read a lagging replica answered), c63c52c1 (the bootstrap asks an invitation again) and 6bb00314 (a
join asks again a sponsor whose name did not resolve); their confirmation at 33 nodes waits on
fedora (below). ab1630ba corrects two comments. The harness (`tools/compare/compose`) reads every
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
50 s. That a heal now reaches every session it can at 33 nodes is the confirmation's to show (below).

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

**Confirmation at 33 nodes: pending.** Two runs on the tree that closes what these runs found,
m6 (twelve outside the control plane killed for 60 s, its placement read every 2 s) and k28 (the
twelve killed for 30 s), on image `focal:fleet-c17` (6bb00314), wait on fedora. m6's first attempt on
`focal:fleet-c16` (01a2f923) stopped in its bootstrap at an agent's invitation (c63c52c1) and its
second at an agent's enrollment (6bb00314); from 10:20 the host's kiln runner restarted every 20 to
30 minutes while other tenants' kiln tests ran there, at loads up to 457, ending every build and run
it carried, the image's three builds among them. The fixes rest until then on their tests and on
CI on Linux, macOS and Windows (01a2f923: runs 41329353459 and 41329353386; 6bb00314: runs
41337828996 and 41337829189).

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

- **The all-copies custody rule** (26 §1): an artifact is acknowledged once every content copy of its
  session holds it, so while a copy is down its artifact steps wait (refused `unavailable` at the
  client's 30 s clock, `artifact.submit` and `validation.report`) or are refused by the custody check
  (`owner_bound` at `validation.begin`); every refusal of the runs above that is not a session without
  its quorum is this rule's. A phase that evaluates an artifact asks every copy again where its
  leader is new, receipts being each leader's own, so in a cut the refusals fall in the sessions
  whose leader was cut off. Whether an acknowledgement needs a quorum of the voters (f+1) instead,
  each of which holds every artifact's object once it has applied the artifact, is the user's
  decision, pending; receipts that outlive their leader would let these begin under either rule.
- **A session without its quorum takes no write** until its voters are back: killed, until they serve
  again after their restart; cut off, until the heal. Its writes begun meanwhile are refused at their
  clocks (`claim.create`) or resent and answered once.
- **A move closes its session to clients** from its cutover until its activation (24 §3). k27's three
  moves came with windows of 4.1, 6.0 and 22.2 s in which their sessions committed nothing, the last
  beside the twelve's restart; the window from cutover to activation alone is not separated (k27's
  placement was not read during the run), which the confirmation's m6 measures.
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
  p9-0639, once in the 69 runs of the three campaigns): the founder's `invite client` ran out the
  admin socket's 20 s while the root group had elected focal2 at term 2 (31 of the 33 nodes named
  it, most at index 612), the founder at term 1 named no leader, and focal3 at term 1 still named
  the founder: neither had heard the new term for the call's 20 s. Not root-caused. One mechanism
  fits every fact and is not shown: a root replica whose own write is pending steps no peer's frame
  until it completes (`ControlHost::must_wait`; frames past `WAITING_FRAMES` are refused), so a
  founder whose write the host's file system held would step no acknowledgement, leave its
  leadership by check-quorum (term 1, no leader) and step none of the new leader's frames, as a
  learner held so would still name the founder; and the founder alone issues invitations, so the
  bootstrap's call waited on it. The nodes' health says nothing of a pending write or of frames
  waiting behind it, which a run must read to confirm or rule it out (the root replica's pending
  write, waiting and let-go frames beside its term and leader).

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
beside it from m5 on (`placement-metrics.txt`). Each run's whole directory, every node's log,
health, metrics and replicas among them, is on fedora under `~/s5/runs/<run>` (the intermediate
runs' too), beside the run kit (`~/s5/campaign.sh`, `fleet-run.sh`, `killreport.py`, `agents.py`,
`gaps.py`, `hostsample.sh`, `placesample.sh`, `btrfssample.sh`, `stallzoom.py`).
