## focal-220-128-work-agents500-nodes33-faults.json

killed (12): focal2 focal9 focal10 focal11 focal12 focal19 focal20 focal23 focal27 focal30 focal31 focal33; all killed by 2.6 s, started again at 68.9 s; serving 69.2 to 71.0 s (12 of 12), 0.3 to 2.1 s after their start

| ledger | session | voters (killed *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the kill) | recovered at (s from kill / from start) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | c9e595b6 | focal1 focal20* focal24 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 452 | 0.8 (96.1–96.9) | 97 / 28.1 |
| 1 | 13d89cb5 | focal4 focal6 focal8 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 384 | 0.9 (96.1–97.0) | 97 / 28.1 |
| 2 | 9d8d929d | focal7 focal18 focal29 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 436 | 0.2 (39.2–39.4) | 79 / 10.1 |
| 3 | 191d34a9 | focal9* focal10* focal17 | quorum | 2,700 | 2,560 | 2,560 | 140 {'owner_bound': 2, 'unavailable': 138} | 0 | 0 | 0 | 710 | 328 | 69.8 (0.2–70.0) | 97 / 28.1 |
| 4 | 79e8fb08 | focal13 focal23* focal30* | quorum | 2,700 | 2,004 | 2,008 | 696 {'unavailable': 696} | 0 | 0 | 0 | 131 | 439 | 65.6 (0.2–65.7) | 87 / 18.1 |
| 5 | 28f1274c | focal11* focal15 focal16 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 365 | 5.7 (50.3–55.9) | 87 / 18.1 |
| 6 | 7550b3a6 | focal19* focal26 focal27* | quorum | 2,700 | 1,964 | 1,964 | 736 {'owner_bound': 9, 'unavailable': 727} | 0 | 0 | 0 | 295 | 343 | 70.6 (0.2–70.8) | 97 / 28.1 |
| 7 | 4caad6fe | focal14 focal21 focal22 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 328 | 0.2 (86.8–87.1) | 89 / 20.1 |
| 8 | 9e74b6a8 | focal3 focal5 focal25 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 367 | 0.9 (96.1–97.0) | 97 / 28.1 |
| 9 | 7d6975e7 | focal12* focal28 focal32 | a follower | 2,700 | 2,066 | 2,066 | 634 {'unavailable': 634} | 0 | 0 | 0 | 142 | 426 | 5.1 (58.5–63.7) | 77 / 8.1 |
| 10 | 9351767b | focal2* focal31* focal33* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 870 | 537 | 67.9 (0.2–68.1) | 97 / 28.1 |

totals: started 29,700, written 27,494, judged 27,498, refused 2,206 {'owner_bound': 11, 'unavailable': 2195}, unknown 0, abandoned 0, undelivered 0, resent 2148; judged/started 92.586%
writes (measured window): committed 275,622, refused 2,206, unknown 0; committed p50/p99/p99.9/max 134.5/20746.0/63563.7/73634.4 ms; schedule delay p99 24.6 ms; wall 162.6 s
refusals (reasons): [['access: Unavailable', 2195], ['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #" }', 9], ['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #,#" }', 2]]

writes not committed, by start (5 s windows from the kill): outcome/cause=count
  [   0,   5) refused/owner_bound=5, refused/unavailable=296
  [   5,  10) refused/unavailable=258
  [  10,  15) refused/unavailable=343
  [  15,  20) refused/unavailable=309
  [  20,  25) refused/unavailable=290
  [  25,  30) refused/unavailable=294
  [  30,  35) refused/unavailable=271
  [  35,  40) refused/unavailable=122
  [  40,  45) refused/unavailable=12
  [  70,  75) refused/owner_bound=6

not committed: 2206; started 0.0 to 71.9 s; latency 0.1 to 60.5 s; steps {10: 41, 4: 603, 1: 1526, 6: 1, 8: 3, 3: 4, 7: 4, 9: 13, 2: 2, 11: 4, 5: 5}

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 0
