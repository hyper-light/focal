## focal-220-128-work-agents500-nodes33-faults.json

isolated (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all cut off by 0.3 s, joined again at 30.9 s (every time from the isolation's start; 'from start' below is from the rejoin)

| ledger | session | voters (cut off *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the isolation) | recovered at (s from the isolation / from the rejoin) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 26824798 | focal1* focal2 focal3 | leader | 2,700 | 2,669 | 2,669 | 31 {'owner_bound': 31} | 0 | 0 | 0 | 21 | 409 | 1.4 (0.3–1.7) | 52 / 21.1 |
| 1 | 758106c0 | focal4 focal15 focal20 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 311 | 0.2 (95.5–95.8) | 98 / 67.2 |
| 2 | 435df300 | focal7* focal8* focal18 | quorum | 2,700 | 2,692 | 2,692 | 8 {'owner_bound': 8} | 0 | 0 | 0 | 117 | 424 | 31.4 (0.3–31.7) | 50 / 19.1 |
| 3 | f3f92947 | focal10 focal14 focal27* | a follower | 2,700 | 2,696 | 2,696 | 4 {'owner_bound': 4} | 0 | 0 | 0 | 297 | 373 | 0.5 (0.9–1.4) | 64 / 33.1 |
| 4 | 4e4b92e2 | focal9* focal13 focal17* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 62 | 346 | 31.2 (0.3–31.4) | 96 / 65.2 |
| 5 | 2d1613a5 | focal6* focal11 focal16* | quorum | 2,700 | 2,699 | 2,699 | 1 {'owner_bound': 1} | 0 | 0 | 0 | 153 | 403 | 31.0 (0.3–31.3) | 64 / 33.1 |
| 6 | cc7a3c15 | focal5 focal19 focal24 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 420 | 0.5 (0.9–1.4) | 65 / 34.1 |
| 7 | cef27ca3 | focal22 focal21 focal29 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 357 | 0.4 (1.1–1.6) | 66 / 35.1 |
| 8 | 76c2c9c1 | focal12 focal23* focal25 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 480 | 0.3 (0.5–0.8) | 50 / 19.1 |
| 9 | a870df43 | focal26 focal28* focal30 | leader | 2,700 | 2,658 | 2,658 | 42 {'owner_bound': 42} | 0 | 0 | 0 | 7 | 345 | 1.3 (0.2–1.6) | 96 / 65.2 |
| 10 | 0665d6c4 | focal31* focal33 focal32* | quorum | 2,700 | 2,696 | 2,696 | 4 {'owner_bound': 4} | 0 | 0 | 0 | 133 | 365 | 31.1 (0.2–31.3) | 64 / 33.1 |

totals: started 29,700, written 29,610, judged 29,610, refused 90 {'owner_bound': 90}, unknown 0, abandoned 0, undelivered 0, resent 790; judged/started 99.697%
writes (measured window): committed 294,034, refused 90, unknown 0; committed p50/p99/p99.9/max 154.1/13357.0/31121.7/38326.2 ms; schedule delay p99 35.5 ms; wall 138.1 s
refusals (reasons): [['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #" }', 89], ['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #,#" }', 1]]

writes not committed, by start (5 s windows from the isolation): outcome/cause=count
  [   0,   5) refused/owner_bound=32
  [   5,  10) refused/owner_bound=6
  [  10,  15) refused/owner_bound=44
  [  30,  35) refused/owner_bound=8

not committed: 90; started 0.1 to 33.3 s; latency 0.0 to 14.9 s; steps {9: 90}

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 0
