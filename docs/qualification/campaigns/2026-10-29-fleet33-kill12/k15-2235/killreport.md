## focal-220-128-work-agents500-nodes33-faults.json

killed (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all killed by 4.7 s, started again at 38.6 s; serving 39.2 to 40.5 s (12 of 12), 0.6 to 1.9 s after their start

| ledger | session | voters (killed *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the kill) | recovered at (s from kill / from start) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | b78daa00 | focal1* focal5 focal33 | leader | 2,700 | 2,639 | 2,639 | 61 {'owner_bound': 8, 'unavailable': 53} | 0 | 0 | 0 | 47 | 193 | 20.2 (20.8–41.0) | 96 / 57.4 |
| 1 | 98cd71e5 | focal4 focal11 focal30 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 183 | 1.0 (28.4–29.4) | 98 / 59.4 |
| 2 | d52a51b3 | focal2 focal7* focal18 | leader | 2,700 | 2,616 | 2,616 | 84 {'owner_bound': 6, 'unavailable': 78} | 0 | 0 | 0 | 65 | 192 | 20.1 (20.8–40.9) | 85 / 46.4 |
| 3 | de250432 | focal10 focal21 focal29 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 283 | 1.0 (28.4–29.4) | 84 / 45.4 |
| 4 | e9c09e80 | focal3 focal13 focal32* | a follower | 2,700 | 2,590 | 2,590 | 110 {'unavailable': 110} | 0 | 0 | 0 | 32 | 183 | 14.6 (15.8–30.4) | 91 / 52.4 |
| 5 | 3174610c | focal16* focal15 focal17* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 201 | 188 | 39.6 (0.2–39.8) | 92 / 53.4 |
| 6 | cc5032f2 | focal8* focal9* focal19 | quorum | 2,700 | 2,533 | 2,533 | 167 {'unavailable': 167} | 0 | 0 | 0 | 38 | 190 | 39.8 (0.2–40.0) | 96 / 57.4 |
| 7 | 5d1961d2 | focal22 focal20 focal27* | a follower | 2,700 | 2,628 | 2,628 | 72 {'unavailable': 72} | 0 | 0 | 0 | 25 | 220 | 14.6 (15.8–30.3) | 84 / 45.4 |
| 8 | bcffb03e | focal12 focal26 focal25 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 190 | 1.1 (28.4–29.4) | 83 / 44.4 |
| 9 | bbf131e1 | focal14 focal24 focal28* | leader | 2,700 | 2,655 | 2,655 | 45 {'owner_bound': 7, 'unavailable': 38} | 0 | 0 | 0 | 57 | 187 | 19.7 (20.8–40.5) | 85 / 46.4 |
| 10 | 2d4c28a1 | focal6* focal23* focal31* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 249 | 192 | 40.5 (0.2–40.8) | 86 / 47.4 |

totals: started 29,700, written 29,161, judged 29,161, refused 539 {'owner_bound': 21, 'unavailable': 518}, unknown 0, abandoned 0, undelivered 0, resent 714; judged/started 98.185%
writes (measured window): committed 287,689, refused 539, unknown 0; committed p50/p99/p99.9/max 161.7/23838.4/34449.1/41647.6 ms; schedule delay p99 16531.0 ms; wall 136.7 s
refusals (reasons): [['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #" }', 21], ['access: Unavailable', 518]]

writes not committed, by start (5 s windows from the kill): outcome/cause=count
  [   0,   5) refused/owner_bound=1, refused/unavailable=161
  [   5,  10) refused/unavailable=138
  [  10,  15) refused/owner_bound=20, refused/unavailable=219

not committed: 539; started 0.2 to 12.9 s; latency 7.4 to 30.2 s; steps {9: 21, 4: 340, 10: 12, 1: 166}

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 0
