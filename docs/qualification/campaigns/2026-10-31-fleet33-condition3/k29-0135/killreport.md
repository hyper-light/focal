## focal-220-128-work-agents500-nodes33-faults.json

killed (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all killed by 3.0 s, started again at 41.1 s; serving 42.5 to 45.1 s (12 of 12), 1.5 to 4.1 s after their start

| ledger | session | voters (killed *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the kill) | recovered at (s from kill / from start) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | a15dacbb | focal1* focal3 focal5 | leader | 2,700 | 2,527 | 2,527 | 173 {'owner_bound': 12, 'unavailable': 161} | 0 | 0 | 0 | 99 | 399 | 1.2 (0.2–1.5) | 98 / 56.9 |
| 1 | 734c4b99 | focal4 focal14 focal15 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 410 | 0.9 (62.9–63.8) | 96 / 54.9 |
| 2 | 187ca9c3 | focal6* focal7* focal20 | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 224 | 371 | 39.9 (0.2–40.2) | 99 / 57.9 |
| 3 | cf4730bb | focal10 focal11 focal12 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 373 | 0.7 (26.2–26.9) | 96 / 54.9 |
| 4 | 0fe65764 | focal8* focal13 focal33 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 351 | 15.6 (72.2–87.9) | 103 / 61.9 |
| 5 | e3c243be | focal2 focal16* focal30 | leader | 2,700 | 2,494 | 2,494 | 206 {'owner_bound': 12, 'unavailable': 194} | 0 | 0 | 0 | 66 | 365 | 2.5 (0.2–2.8) | 98 / 56.9 |
| 6 | fe2086e9 | focal19 focal26 focal27* | a follower | 2,700 | 2,535 | 2,535 | 165 {'unavailable': 165} | 0 | 0 | 0 | 148 | 362 | 0.7 (26.2–26.9) | 98 / 56.9 |
| 7 | e7f786c1 | focal22 focal24 focal32* | a follower | 2,700 | 2,563 | 2,563 | 137 {'ingress_capacity': 40, 'unavailable': 97} | 0 | 0 | 0 | 133 | 481 | 2.8 (12.8–15.6) | 96 / 54.9 |
| 8 | fb206165 | focal18 focal25 focal29 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 397 | 0.9 (62.9–63.8) | 98 / 56.9 |
| 9 | 7a2f8a69 | focal21 focal23* focal28* | quorum | 2,700 | 2,664 | 2,664 | 36 {'owner_bound': 4, 'unavailable': 32} | 0 | 0 | 0 | 269 | 384 | 43.2 (0.2–43.5) | 96 / 54.9 |
| 10 | 49be1e6b | focal9* focal17* focal31* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 443 | 490 | 41.5 (0.2–41.8) | 96 / 54.9 |

totals: started 29,700, written 28,983, judged 28,983, refused 717 {'owner_bound': 28, 'ingress_capacity': 40, 'unavailable': 649}, unknown 0, abandoned 0, undelivered 0, resent 1382; judged/started 97.586%
writes (measured window): committed 285,781, refused 717, unknown 0; committed p50/p99/p99.9/max 259.3/18053.9/40097.7/46449.8 ms; schedule delay p99 96.7 ms; wall 144.6 s
refusals (reasons): [['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #" }', 28], ['access: Unavailable', 649], ['access: Capacity', 40]]

writes not committed, by start (5 s windows from the kill): outcome/cause=count
  [   0,   5) refused/owner_bound=13, refused/unavailable=89
  [   5,  10) refused/owner_bound=4, refused/unavailable=221
  [  10,  15) refused/ingress_capacity=38, refused/owner_bound=11, refused/unavailable=337
  [  15,  20) refused/ingress_capacity=2, refused/unavailable=2

not committed: 717; started 0.2 to 15.1 s; latency 0.0 to 30.3 s; steps {9: 28, 4: 604, 1: 67, 10: 13, 2: 2, 3: 3}

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 0
