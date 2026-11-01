## focal-220-128-work-agents500-nodes33-faults.json

isolated (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all cut off by 0.1 s, joined again at 12.3 s (every time from the isolation's start; 'from start' below is from the rejoin)

| ledger | session | voters (cut off *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the isolation) | recovered at (s from the isolation / from the rejoin) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 4afba5d4 | focal1* focal5 focal12 | leader | 2,700 | 2,677 | 2,677 | 23 {'owner_bound': 23} | 0 | 0 | 0 | 0 | 1515 | 1.4 (0.1–1.5) | 16 / 3.7 |
| 1 | 63f4c3b3 | focal4 focal11 focal15 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 253 | 0.5 (0.1–0.6) | 87 / 74.7 |
| 2 | a8ff840d | focal7* focal27* focal29 | quorum | 2,700 | 2,699 | 2,699 | 1 {'owner_bound': 1} | 0 | 0 | 0 | 0 | 14804 | 13.8 (0.1–13.9) | 1 / -11.3 |
| 3 | c850ed1b | focal10 focal24 focal32* | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 254 | 0.5 (0.7–1.2) | 92 / 79.7 |
| 4 | ec677587 | focal13 focal20 focal33 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 255 | 0.5 (0.5–0.9) | 93 / 80.7 |
| 5 | 904112be | focal8* focal9* focal16* | quorum | 2,700 | 2,676 | 2,676 | 24 {'owner_bound': 24} | 0 | 0 | 0 | 31 | 14036 | 13.0 (0.1–13.1) | 16 / 3.7 |
| 6 | f461e3fd | focal3 focal19 focal26 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 262 | 0.5 (0.2–0.7) | 57 / 44.7 |
| 7 | 03610afd | focal2 focal6* focal22 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 245 | 0.4 (0.7–1.1) | 52 / 39.7 |
| 8 | a3fedb42 | focal18 focal23* focal25 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 236 | 0.5 (0.6–1.1) | 94 / 81.7 |
| 9 | 9c47f097 | focal17* focal21 focal28* | quorum | 2,700 | 2,695 | 2,695 | 5 {'owner_bound': 5} | 0 | 0 | 0 | 0 | 14665 | 13.5 (0.1–13.6) | 1 / -11.3 |
| 10 | dbcd7aef | focal14 focal30 focal31* | leader | 2,700 | 2,679 | 2,679 | 21 {'owner_bound': 21} | 0 | 0 | 0 | 0 | 240 | 1.4 (0.1–1.5) | 47 / 34.7 |

totals: started 29,700, written 29,626, judged 29,626, refused 74 {'owner_bound': 74}, unknown 0, abandoned 0, undelivered 0, resent 31; judged/started 99.751%
writes (measured window): committed 291,210, refused 74, unknown 0; committed p50/p99/p99.9/max 91.9/582.9/11789.3/16720.9 ms; schedule delay p99 2.8 ms; wall 137.6 s
refusals (reasons): [['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #" }', 69], ['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #,#" }', 5]]

writes not committed, by start (5 s windows from the isolation): outcome/cause=count
  [  -5,   0) refused/owner_bound=1
  [   0,   5) refused/owner_bound=30
  [   5,  10) refused/owner_bound=4
  [  10,  15) refused/owner_bound=29
  [  15,  20) refused/owner_bound=10

not committed: 74; started -0.0 to 15.9 s; latency 0.0 to 13.8 s; steps {9: 74}

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 0
