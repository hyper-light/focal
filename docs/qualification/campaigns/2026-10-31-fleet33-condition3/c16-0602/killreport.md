## focal-220-128-work-agents500-nodes33-faults.json

isolated (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all cut off by 0.1 s, joined again at 12.3 s (every time from the isolation's start; 'from start' below is from the rejoin)

| ledger | session | voters (cut off *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the isolation) | recovered at (s from the isolation / from the rejoin) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | ddb4b59a | focal1* focal14 focal15 | leader | 2,700 | 2,677 | 2,677 | 23 {'owner_bound': 23} | 0 | 0 | 0 | 0 | 16174 | 1.8 (0.0–1.8) | 13 / 0.7 |
| 1 | 4cdb5de4 | focal4 focal26 focal33 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 261 | 0.5 (0.7–1.2) | 42 / 29.6 |
| 2 | 2d1c5363 | focal7* focal23* focal27* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 33 | 16986 | 12.8 (0.1–12.9) | 0 / -12.3 |
| 3 | 35d4f25f | focal10 focal24 focal32* | a follower | 2,700 | 2,697 | 2,697 | 3 {'owner_bound': 3} | 0 | 0 | 0 | 0 | 224 | 0.6 (1.0–1.6) | 78 / 65.7 |
| 4 | 5e1f3675 | focal3 focal8* focal13 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 279 | 0.4 (0.7–1.1) | 43 / 30.6 |
| 5 | bc2403bd | focal16* focal20 focal21 | leader | 2,700 | 2,667 | 2,667 | 33 {'owner_bound': 33} | 0 | 0 | 0 | 0 | 13928 | 1.9 (0.1–1.9) | 12 / -0.3 |
| 6 | bd48fd3b | focal17* focal18 focal19 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 272 | 0.5 (0.6–1.1) | 46 / 33.6 |
| 7 | 5d0b625a | focal22 focal29 focal30 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 258 | 0.4 (0.7–1.2) | 43 / 30.6 |
| 8 | aa1e5036 | focal2 focal12 focal25 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 245 | 0.5 (0.3–0.7) | 83 / 70.7 |
| 9 | 8d311f98 | focal9* focal11 focal28* | quorum | 2,700 | 2,697 | 2,697 | 3 {'owner_bound': 3} | 0 | 0 | 0 | 0 | 15647 | 14.7 (0.1–14.8) | 0 / -12.3 |
| 10 | 7ec4be23 | focal5 focal6* focal31* | quorum | 2,700 | 2,694 | 2,694 | 6 {'owner_bound': 6} | 0 | 0 | 0 | 0 | 16023 | 15.1 (0.0–15.2) | 1 / -11.3 |

totals: started 29,700, written 29,632, judged 29,632, refused 68 {'owner_bound': 68}, unknown 0, abandoned 0, undelivered 0, resent 33; judged/started 99.771%
writes (measured window): committed 293,719, refused 68, unknown 0; committed p50/p99/p99.9/max 113.7/589.0/12088.7/16985.8 ms; schedule delay p99 6.3 ms; wall 137.9 s
refusals (reasons): [['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #" }', 59], ['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #,#" }', 9]]

writes not committed, by start (5 s windows from the isolation): outcome/cause=count
  [  -5,   0) refused/owner_bound=4
  [   0,   5) refused/owner_bound=38
  [   5,  10) refused/owner_bound=5
  [  10,  15) refused/owner_bound=21

not committed: 68; started -0.0 to 12.1 s; latency 0.0 to 12.9 s; steps {9: 68}

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 0
