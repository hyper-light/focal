## focal-220-128-work-agents500-nodes33-faults.json

killed (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all killed by 1.1 s, started again at 35.3 s; serving 35.9 to 40.4 s (12 of 12), 0.6 to 5.1 s after their start

| ledger | session | voters (killed *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the kill) | recovered at (s from kill / from start) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 4940bb9a | focal1* focal15 focal32* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 156 | 34.9 (0.2–35.1) | 95 / 59.7 |
| 1 | 2c588ea3 | focal4 focal5 focal33 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 133 | 0.5 (1.1–1.6) | 94 / 58.7 |
| 2 | 509c64ba | focal7* focal9* focal17* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 341 | 140 | 37.7 (0.2–37.9) | 94 / 58.7 |
| 3 | 423d6dd5 | focal2 focal3 focal10 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 141 | 0.5 (0.3–0.8) | 95 / 59.7 |
| 4 | c4ec4677 | focal11 focal13 focal27* | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 144 | 0.5 (0.8–1.3) | 94 / 58.7 |
| 5 | 7a8f0c96 | focal16* focal18 focal26 | leader | 2,700 | 2,692 | 2,692 | 8 {'owner_bound': 8} | 0 | 0 | 0 | 0 | 158 | 1.8 (0.2–1.9) | 72 / 36.7 |
| 6 | 4b892957 | focal12 focal20 focal19 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 144 | 0.6 (50.8–51.4) | 95 / 59.7 |
| 7 | 8204719f | focal21 focal23* focal22 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 145 | 0.5 (0.9–1.4) | 94 / 58.7 |
| 8 | 45ec148a | focal6* focal8* focal25 | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 143 | 35.4 (0.2–35.5) | 90 / 54.7 |
| 9 | af772079 | focal28* focal29 focal30 | leader | 2,700 | 2,691 | 2,691 | 9 {'owner_bound': 9} | 0 | 0 | 0 | 0 | 169 | 1.7 (0.2–1.9) | 72 / 36.7 |
| 10 | b8b108f4 | focal14 focal24 focal31* | leader | 2,700 | 2,693 | 2,693 | 7 {'owner_bound': 7} | 0 | 0 | 0 | 0 | 142 | 1.5 (0.2–1.7) | 95 / 59.7 |

totals: started 29,700, written 29,676, judged 29,676, refused 24 {'owner_bound': 24}, unknown 0, abandoned 0, undelivered 0, resent 341; judged/started 99.919%
writes (measured window): committed 291,285, refused 24, unknown 0; committed p50/p99/p99.9/max 82.2/16066.4/34494.4/42287.1 ms; schedule delay p99 48.9 ms; wall 136.7 s
refusals (reasons): [['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact, every copy up and a majority needed; missing nodes ; down nodes #" }', 24]]

writes not committed, by start (5 s windows from the kill): outcome/cause=count
  [   0,   5) refused/owner_bound=15
  [   5,  10) refused/owner_bound=1
  [  10,  15) refused/owner_bound=8

not committed: 24; started 2.2 to 12.4 s; latency 0.0 to 2.5 s; steps {9: 24}

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 0
