## focal-220-128-work-agents500-nodes33-faults.json

killed (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all killed by 3.9 s, started again at 41.5 s; serving 41.9 to 44.4 s (12 of 12), 0.4 to 3.0 s after their start

| ledger | session | voters (killed *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the kill) | recovered at (s from kill / from start) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | a5f9d90a | focal1* focal11 focal27* | quorum | 2,700 | 2,688 | 2,688 | 12 {'unavailable': 12} | 0 | 0 | 0 | 276 | 461 | 41.1 (0.2–41.3) | 97 / 55.5 |
| 1 | f621db1e | focal3 focal4 focal5 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 257 | 0.6 (76.0–76.6) | 97 / 55.5 |
| 2 | 90a29939 | focal2 focal7* focal12 | leader | 2,700 | 2,526 | 2,526 | 174 {'owner_bound': 25, 'unavailable': 149} | 0 | 0 | 0 | 138 | 330 | 1.6 (0.2–1.8) | 97 / 55.5 |
| 3 | d09162c5 | focal10 focal23* focal24 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 241 | 0.5 (0.6–1.1) | 97 / 55.5 |
| 4 | bc1b0748 | focal13 focal30 focal32* | a follower | 2,700 | 2,535 | 2,535 | 165 {'owner_bound': 3, 'unavailable': 162} | 0 | 0 | 0 | 180 | 295 | 0.4 (0.8–1.3) | 97 / 55.5 |
| 5 | c3f1b188 | focal16* focal26 focal33 | leader | 2,700 | 2,622 | 2,622 | 78 {'owner_bound': 22, 'unavailable': 56} | 0 | 0 | 0 | 114 | 526 | 2.4 (0.2–2.6) | 97 / 55.5 |
| 6 | 1816817b | focal6* focal14 focal19 | a follower | 2,700 | 2,469 | 2,469 | 231 {'unavailable': 231} | 0 | 0 | 0 | 121 | 253 | 7.5 (68.2–75.7) | 97 / 55.5 |
| 7 | 7f0a690a | focal15 focal20 focal22 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 301 | 0.4 (1.3–1.7) | 98 / 56.5 |
| 8 | 28d3b5cc | focal8* focal18 focal25 | a follower | 2,700 | 2,657 | 2,657 | 43 {'unavailable': 43} | 0 | 0 | 0 | 153 | 299 | 10.8 (76.5–87.2) | 97 / 55.5 |
| 9 | be4090b0 | focal21 focal28* focal29 | leader | 2,700 | 2,673 | 2,673 | 27 {'owner_bound': 19, 'unavailable': 8} | 0 | 0 | 0 | 148 | 265 | 1.9 (0.2–2.1) | 99 / 57.5 |
| 10 | 4878bc4d | focal9* focal17* focal31* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 469 | 257 | 42.0 (0.1–42.1) | 97 / 55.5 |

totals: started 29,700, written 28,970, judged 28,970, refused 730 {'owner_bound': 69, 'unavailable': 661}, unknown 0, abandoned 0, undelivered 0, resent 1599; judged/started 97.542%
writes (measured window): committed 286,506, refused 730, unknown 0; committed p50/p99/p99.9/max 173.3/22235.1/40636.5/47267.0 ms; schedule delay p99 53.3 ms; wall 138.3 s
refusals (reasons): [['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #" }', 69], ['access: Unavailable', 661]]

writes not committed, by start (5 s windows from the kill): outcome/cause=count
  [   0,   5) refused/owner_bound=43, refused/unavailable=56
  [   5,  10) refused/owner_bound=8, refused/unavailable=249
  [  10,  15) refused/owner_bound=18, refused/unavailable=346
  [  15,  20) refused/unavailable=10

not committed: 730; started 0.1 to 15.7 s; latency 0.0 to 30.2 s; steps {1: 12, 9: 69, 4: 617, 10: 32}

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 0
