## focal-220-128-work-agents500-nodes33-faults.json

isolated (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all cut off by 0.2 s, joined again at 12.4 s (every time from the isolation's start; 'from start' below is from the rejoin)

| ledger | session | voters (cut off *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the isolation) | recovered at (s from the isolation / from the rejoin) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 2445fc26 | focal1* focal5 focal15 | leader | 2,700 | 2,692 | 2,692 | 8 {'owner_bound': 8} | 0 | 0 | 0 | 0 | 143 | 1.6 (0.2–1.8) | 95 / 82.6 |
| 1 | a33ce8cb | focal4 focal6* focal11 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 142 | 1.4 (11.7–13.1) | 95 / 82.6 |
| 2 | 205cde29 | focal7* focal23* focal30 | quorum | 2,700 | 2,699 | 2,699 | 1 {'owner_bound': 1} | 0 | 0 | 0 | 0 | 150 | 13.2 (0.2–13.4) | 94 / 81.6 |
| 3 | 96bc77a5 | focal2 focal10 focal33 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 156 | 6.5 (66.0–72.5) | 93 / 80.6 |
| 4 | d6ceffcb | focal12 focal13 focal14 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 146 | 1.4 (11.7–13.1) | 95 / 82.6 |
| 5 | 3c11b8a3 | focal16* focal18 focal20 | leader | 2,700 | 2,695 | 2,695 | 5 {'owner_bound': 5} | 0 | 0 | 0 | 0 | 162 | 2.5 (0.2–2.7) | 94 / 81.6 |
| 6 | f57bf16e | focal3 focal19 focal32* | a follower | 2,700 | 2,688 | 2,688 | 12 {'ingress_capacity': 12} | 0 | 0 | 0 | 0 | 151 | 1.8 (11.2–13.1) | 95 / 82.6 |
| 7 | bf4763b0 | focal8* focal9* focal22 | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 159 | 13.0 (0.2–13.1) | 94 / 81.6 |
| 8 | 8a495b5d | focal25 focal26 focal27* | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 214 | 1.4 (11.7–13.1) | 84 / 71.6 |
| 9 | 08e92658 | focal17* focal24 focal28* | quorum | 2,700 | 2,699 | 2,699 | 1 {'owner_bound': 1} | 0 | 0 | 0 | 0 | 146 | 13.4 (0.2–13.5) | 95 / 82.6 |
| 10 | 53775e51 | focal21 focal29 focal31* | leader | 2,700 | 2,691 | 2,691 | 9 {'owner_bound': 9} | 0 | 0 | 0 | 0 | 149 | 2.9 (11.7–14.6) | 95 / 82.6 |

totals: started 29,700, written 29,664, judged 29,664, refused 36 {'owner_bound': 24, 'ingress_capacity': 12}, unknown 0, abandoned 0, undelivered 0, resent 0; judged/started 99.879%
writes (measured window): committed 291,218, refused 36, unknown 0; committed p50/p99/p99.9/max 84.7/930.9/11396.3/16469.3 ms; schedule delay p99 12.2 ms; wall 137.1 s
refusals (reasons): [['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #" }', 22], ['access: Capacity', 12], ['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #,#" }', 2]]

writes not committed, by start (5 s windows from the isolation): outcome/cause=count
  [   0,   5) refused/owner_bound=17
  [   5,  10) refused/owner_bound=1
  [  10,  15) refused/ingress_capacity=12, refused/owner_bound=6

not committed: 36; started 0.1 to 14.7 s; latency 0.0 to 11.1 s; steps {9: 24, 1: 12}

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 0
