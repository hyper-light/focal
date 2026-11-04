## focal-220-128-work-agents500-nodes33-faults.json

killed (12): focal2 focal9 focal10 focal11 focal12 focal19 focal20 focal23 focal27 focal30 focal31 focal33; all killed by 1.6 s, started again at 65.8 s; serving 66.6 to 69.3 s (12 of 12), 0.8 to 3.5 s after their start

| ledger | session | voters (killed *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the kill) | recovered at (s from kill / from start) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 47000ab8 | focal1 focal3 focal5 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 139 | 0.4 (79.5–79.9) | 96 / 30.2 |
| 1 | 3aa45dd7 | focal4 focal29 focal30* | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 137 | 6.5 (45.9–52.3) | 95 / 29.2 |
| 2 | 78fd8646 | focal7 focal8 focal24 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 149 | 0.3 (79.5–79.8) | 95 / 29.2 |
| 3 | d9637347 | focal10* focal17 focal27* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 143 | 68.3 (0.3–68.6) | 95 / 29.2 |
| 4 | 5d1a7b1b | focal13 focal32 focal33* | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 139 | 13.3 (51.8–65.1) | 95 / 29.2 |
| 5 | 7f8004ee | focal12* focal16 focal20* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 166 | 65.9 (0.3–66.2) | 94 / 28.2 |
| 6 | 7216d544 | focal2* focal6 focal19* | quorum | 2,700 | 2,697 | 2,697 | 3 {'owner_bound': 3} | 0 | 0 | 0 | 0 | 149 | 64.6 (0.3–64.9) | 95 / 29.2 |
| 7 | a2f70a7c | focal11* focal15 focal22 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 126 | 24.8 (51.8–76.5) | 99 / 33.2 |
| 8 | d69280cb | focal14 focal18 focal25 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 161 | 0.3 (79.5–79.8) | 95 / 29.2 |
| 9 | fda15e4b | focal21 focal23* focal28 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 140 | 17.4 (51.8–69.1) | 95 / 29.2 |
| 10 | eb847642 | focal9* focal26 focal31* | quorum | 2,700 | 2,699 | 2,699 | 1 {'owner_bound': 1} | 0 | 0 | 0 | 0 | 136 | 66.9 (0.3–67.1) | 95 / 29.2 |

totals: started 29,700, written 29,696, judged 29,696, refused 4 {'owner_bound': 4}, unknown 0, abandoned 0, undelivered 0, resent 0; judged/started 99.987%
writes (measured window): committed 291,416, refused 4, unknown 0; committed p50/p99/p99.9/max 281.0/48649.2/66535.2/77296.2 ms; schedule delay p99 9821.9 ms; wall 140.8 s
refusals (reasons): [['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact, every copy up and a majority needed; missing nodes ; down nodes #" }', 2], ['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact, every copy up and a majority needed; missing nodes ; down nodes #,#', 2]]

writes not committed, by start (5 s windows from the kill): outcome/cause=count
  [   0,   5) refused/owner_bound=3
  [  65,  70) refused/owner_bound=1

not committed: 4; started 0.2 to 67.2 s; latency 0.5 to 21.2 s; steps {9: 4}

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 0
