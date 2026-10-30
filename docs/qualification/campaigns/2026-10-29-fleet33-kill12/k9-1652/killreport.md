## focal-220-128-work-agents500-nodes33-faults.json

killed (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all killed by 1.2 s, started again at 34.0 s; serving 34.3 to 36.8 s (12 of 12), 0.3 to 2.8 s after their start

| ledger | session | voters (killed *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the kill) | recovered at (s from kill / from start) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | d3b5f4a2 | focal1* focal20 focal24 | leader | 2,700 | 2,696 | 2,696 | 4 {'owner_bound': 4} | 0 | 0 | 0 | 4 | 92 | 10.0 (0.2–10.2) | 89 / 55.0 |
| 1 | 7a2c5f24 | focal4 focal8* focal18 | a follower | 2,700 | 2,640 | 2,640 | 60 {'unavailable': 60} | 0 | 0 | 0 | 8 | 145 | 7.9 (22.4–30.3) | 93 / 59.0 |
| 2 | 20d8e448 | focal3 focal7* focal23* | quorum | 2,700 | 2,699 | 2,699 | 1 {'owner_bound': 1} | 0 | 0 | 0 | 150 | 148 | 36.6 (0.2–36.8) | 79 / 45.0 |
| 3 | e694256b | focal10 focal14 focal15 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 141 | 0.7 (77.9–78.6) | 79 / 45.0 |
| 4 | a08dfa49 | focal13 focal17* focal33 | a follower | 2,700 | 2,660 | 2,660 | 40 {'unavailable': 40} | 0 | 0 | 0 | 1 | 134 | 7.9 (22.3–30.2) | 91 / 57.0 |
| 5 | ef5a8f8d | focal5 focal16* focal21 | leader | 2,700 | 2,696 | 2,696 | 4 {'owner_bound': 4} | 0 | 0 | 0 | 2 | 138 | 10.0 (0.2–10.2) | 79 / 45.0 |
| 6 | 022b7295 | focal6* focal19 focal29 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 168 | 7.4 (22.9–30.3) | 79 / 45.0 |
| 7 | 4c09cb00 | focal2 focal22 focal30 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 145 | 0.7 (77.9–78.6) | 79 / 45.0 |
| 8 | 54c87888 | focal9* focal11 focal25 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 144 | 7.4 (22.9–30.3) | 79 / 45.0 |
| 9 | 93850c49 | focal12 focal28* focal32* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 131 | 124 | 36.2 (0.2–36.4) | 80 / 46.0 |
| 10 | 62b9c603 | focal26 focal27* focal31* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 128 | 133 | 36.1 (0.2–36.3) | 79 / 45.0 |

totals: started 29,700, written 29,591, judged 29,591, refused 109 {'owner_bound': 9, 'unavailable': 100}, unknown 0, abandoned 0, undelivered 0, resent 424; judged/started 99.633%
writes (measured window): committed 290,654, refused 109, unknown 0; committed p50/p99/p99.9/max 38.3/16012.5/32597.1/38711.2 ms; schedule delay p99 11228.8 ms; wall 149.8 s
refusals (reasons): [['access: Unavailable', 100], ['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #" }', 8], ['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #,#" }', 1]]

writes not committed, by start (5 s windows from the kill): outcome/cause=count
  [   0,   5) refused/owner_bound=4, refused/unavailable=75
  [   5,  10) refused/unavailable=25
  [  10,  15) refused/owner_bound=3
  [  20,  25) refused/owner_bound=2

not committed: 109; started 0.2 to 22.0 s; latency 8.4 to 30.0 s; steps {9: 9, 4: 92, 10: 8}

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 1
  focal9 54c87888: native: native lifecycle admission: a configured bound or counter would be exceeded
