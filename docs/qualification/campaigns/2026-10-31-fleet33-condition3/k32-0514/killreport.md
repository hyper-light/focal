## focal-220-128-work-agents500-nodes33-faults.json

killed (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all killed by 1.7 s, started again at 37.8 s; serving 38.5 to 41.4 s (12 of 12), 0.7 to 3.6 s after their start

| ledger | session | voters (killed *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the kill) | recovered at (s from kill / from start) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | a5b318b4 | focal1* focal11 focal18 | leader | 2,700 | 2,536 | 2,536 | 164 {'owner_bound': 10, 'unavailable': 154} | 0 | 0 | 0 | 95 | 179 | 7.3 (50.6–57.9) | 97 / 59.2 |
| 1 | 2e1d6455 | focal4 focal21 focal26 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 158 | 5.6 (50.6–56.2) | 96 / 58.2 |
| 2 | 810dd827 | focal7* focal29 focal33 | leader | 2,700 | 2,619 | 2,619 | 81 {'owner_bound': 12, 'unavailable': 69} | 0 | 0 | 0 | 79 | 193 | 6.9 (50.6–57.5) | 97 / 59.2 |
| 3 | f871a530 | focal10 focal30 focal32* | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 145 | 7.7 (50.6–58.3) | 97 / 59.2 |
| 4 | 1ba15dac | focal6* focal13 focal17* | quorum | 2,700 | 2,630 | 2,630 | 70 {'unavailable': 70} | 0 | 0 | 0 | 84 | 147 | 36.9 (0.2–37.0) | 97 / 59.2 |
| 5 | 0341252f | focal9* focal14 focal16* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 157 | 227 | 36.7 (0.2–36.8) | 97 / 59.2 |
| 6 | 572030c0 | focal12 focal19 focal23* | a follower | 2,700 | 2,621 | 2,621 | 79 {'unavailable': 79} | 0 | 0 | 0 | 36 | 214 | 13.3 (78.5–91.8) | 97 / 59.2 |
| 7 | 7f067d0c | focal20 focal22 focal27* | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 280 | 10.1 (93.6–103.8) | 94 / 56.2 |
| 8 | e8fd4fbc | focal5 focal15 focal25 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 150 | 6.6 (50.6–57.2) | 97 / 59.2 |
| 9 | a06cf2dd | focal2 focal3 focal28* | leader | 2,700 | 2,614 | 2,614 | 86 {'owner_bound': 11, 'unavailable': 75} | 0 | 0 | 0 | 28 | 141 | 4.1 (50.6–54.7) | 97 / 59.2 |
| 10 | 62d1ad6d | focal8* focal24 focal31* | quorum | 2,700 | 2,699 | 2,699 | 1 {'owner_bound': 1} | 0 | 0 | 0 | 199 | 200 | 39.4 (0.2–39.6) | 96 / 58.2 |

totals: started 29,700, written 29,219, judged 29,219, refused 481 {'owner_bound': 34, 'unavailable': 447}, unknown 0, abandoned 0, undelivered 0, resent 678; judged/started 98.380%
writes (measured window): committed 287,793, refused 481, unknown 0; committed p50/p99/p99.9/max 296.3/15605.8/34781.5/42151.1 ms; schedule delay p99 83.2 ms; wall 147.3 s
refusals (reasons): [['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #" }', 34], ['access: Unavailable', 447]]

writes not committed, by start (5 s windows from the kill): outcome/cause=count
  [   0,   5) refused/owner_bound=16, refused/unavailable=152
  [   5,  10) refused/owner_bound=5, refused/unavailable=213
  [  10,  15) refused/owner_bound=12, refused/unavailable=82
  [  35,  40) refused/owner_bound=1

not committed: 481; started 0.1 to 39.5 s; latency 0.0 to 30.2 s; steps {9: 34, 4: 365, 10: 12, 1: 70}

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 0
