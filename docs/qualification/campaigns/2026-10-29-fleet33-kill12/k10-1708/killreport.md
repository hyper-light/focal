## focal-220-128-work-agents500-nodes33-faults.json

killed (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all killed by 1.1 s, started again at 34.0 s; serving 35.5 to 36.9 s (12 of 12), 1.5 to 2.9 s after their start

| ledger | session | voters (killed *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the kill) | recovered at (s from kill / from start) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | e2a93b02 | focal1* focal26 focal30 | leader | 2,700 | 2,692 | 2,692 | 8 {'owner_bound': 7, 'unavailable': 1} | 0 | 0 | 0 | 0 | 155 | 10.0 (0.2–10.3) | 78 / 44.0 |
| 1 | 9b306869 | focal2 focal4 focal27* | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 138 | 7.6 (22.7–30.3) | 78 / 44.0 |
| 2 | 5433aa37 | focal7* focal12 focal29 | leader | 2,700 | 2,691 | 2,691 | 9 {'owner_bound': 9} | 0 | 0 | 0 | 0 | 121 | 10.0 (0.2–10.3) | 92 / 58.0 |
| 3 | 64bb10c5 | focal10 focal20 focal24 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 124 | 0.3 (0.7–1.0) | 80 / 46.0 |
| 4 | fed2a385 | focal13 focal18 focal14 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 149 | 0.5 (77.3–77.9) | 78 / 44.0 |
| 5 | 49536733 | focal16* focal23* focal33 | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 132 | 135 | 36.4 (0.2–36.6) | 78 / 44.0 |
| 6 | a9d2ca7e | focal8* focal19 focal21 | a follower | 2,700 | 2,649 | 2,649 | 51 {'unavailable': 51} | 0 | 0 | 0 | 263 | 125 | 7.3 (22.9–30.2) | 78 / 44.0 |
| 7 | e329271c | focal11 focal15 focal22 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 125 | 0.5 (77.3–77.9) | 78 / 44.0 |
| 8 | 5d3811eb | focal3 focal25 focal32* | a follower | 2,700 | 2,627 | 2,627 | 73 {'unavailable': 73} | 0 | 0 | 0 | 0 | 137 | 7.9 (22.4–30.3) | 80 / 46.0 |
| 9 | 3ae490ee | focal9* focal17* focal28* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 184 | 122 | 37.8 (0.2–38.0) | 78 / 44.0 |
| 10 | 3234768e | focal6* focal5 focal31* | quorum | 2,700 | 2,699 | 2,699 | 1 {'owner_bound': 1} | 0 | 0 | 0 | 132 | 140 | 36.2 (0.2–36.4) | 78 / 44.0 |

totals: started 29,700, written 29,558, judged 29,558, refused 142 {'owner_bound': 17, 'unavailable': 125}, unknown 0, abandoned 0, undelivered 0, resent 711; judged/started 99.522%
writes (measured window): committed 290,490, refused 142, unknown 0; committed p50/p99/p99.9/max 38.8/17272.1/33275.7/40503.2 ms; schedule delay p99 11021.0 ms; wall 135.8 s
refusals (reasons): [['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #" }', 16], ['access: Unavailable', 125], ['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #,#" }', 1]]

writes not committed, by start (5 s windows from the kill): outcome/cause=count
  [   0,   5) refused/owner_bound=3, refused/unavailable=75
  [   5,  10) refused/unavailable=45
  [  10,  15) refused/owner_bound=7, refused/unavailable=5
  [  20,  25) refused/owner_bound=4
  [  30,  35) refused/owner_bound=3

not committed: 142; started 0.2 to 32.2 s; latency 0.0 to 30.0 s; steps {9: 17, 4: 117, 10: 8}

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 0
