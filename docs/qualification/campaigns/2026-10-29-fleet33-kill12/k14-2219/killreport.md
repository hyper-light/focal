## focal-220-128-work-agents500-nodes33-faults.json

killed (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all killed by 1.5 s, started again at 34.5 s; serving 35.0 to 36.2 s (12 of 12), 0.5 to 1.7 s after their start

| ledger | session | voters (killed *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the kill) | recovered at (s from kill / from start) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 6099bf0e | focal1* focal26 focal33 | leader | 2,700 | 2,692 | 2,692 | 8 {'owner_bound': 8} | 0 | 0 | 0 | 2 | 905 | 16.0 (20.4–36.5) | 85 / 50.5 |
| 1 | a81d63d8 | focal4 focal20 focal21 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 888 | 0.6 (68.4–69.0) | 85 / 50.5 |
| 2 | d42cd2ef | focal8* focal7* focal27* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 215 | 554 | 36.1 (0.2–36.2) | 89 / 54.5 |
| 3 | b323df2e | focal10 focal15 focal23* | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 918 | 0.7 (0.4–1.1) | 85 / 50.5 |
| 4 | 5ce27a5c | focal3 focal11 focal13 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 913 | 0.7 (48.3–49.0) | 84 / 49.5 |
| 5 | 2a4afd75 | focal16* focal24 focal32* | quorum | 2,700 | 2,699 | 2,699 | 1 {'owner_bound': 1} | 0 | 0 | 0 | 130 | 547 | 36.2 (0.2–36.4) | 89 / 54.5 |
| 6 | bd886237 | focal2 focal12 focal19 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 552 | 0.6 (68.4–69.0) | 88 / 53.5 |
| 7 | 2c24fd96 | focal5 focal9* focal22 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 677 | 0.6 (0.5–1.1) | 85 / 50.5 |
| 8 | 740e77cd | focal6* focal17* focal25 | quorum | 2,700 | 2,603 | 2,603 | 97 {'unavailable': 97} | 0 | 0 | 0 | 10 | 552 | 35.0 (0.2–35.2) | 89 / 54.5 |
| 9 | c5ad81f5 | focal14 focal18 focal28* | leader | 2,700 | 2,692 | 2,692 | 8 {'owner_bound': 8} | 0 | 0 | 0 | 1 | 1043 | 17.0 (20.5–37.5) | 51 / 16.5 |
| 10 | 7b876b40 | focal30 focal29 focal31* | leader | 2,700 | 2,693 | 2,693 | 7 {'owner_bound': 7} | 0 | 0 | 0 | 3 | 893 | 15.3 (20.4–35.7) | 85 / 50.5 |

totals: started 29,700, written 29,579, judged 29,579, refused 121 {'owner_bound': 24, 'unavailable': 97}, unknown 0, abandoned 0, undelivered 0, resent 361; judged/started 99.593%
writes (measured window): committed 290,146, refused 121, unknown 0; committed p50/p99/p99.9/max 91.9/15817.0/29777.5/37594.5 ms; schedule delay p99 5713.1 ms; wall 137.8 s
refusals (reasons): [['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #" }', 23], ['access: Unavailable', 97], ['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #,#" }', 1]]

writes not committed, by start (5 s windows from the kill): outcome/cause=count
  [   0,   5) refused/owner_bound=4, refused/unavailable=91
  [   5,  10) refused/unavailable=6
  [  10,  15) refused/owner_bound=20

not committed: 121; started 0.1 to 13.0 s; latency 7.2 to 30.0 s; steps {9: 24, 1: 97}

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 0
