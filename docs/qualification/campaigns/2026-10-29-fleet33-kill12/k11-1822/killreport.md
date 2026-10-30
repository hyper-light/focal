## focal-220-128-work-agents500-nodes33-faults.json

killed (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all killed by 1.8 s, started again at 38.2 s; serving 38.5 to 39.8 s (12 of 12), 0.3 to 1.6 s after their start

| ledger | session | voters (killed *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the kill) | recovered at (s from kill / from start) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0b5cf797 | focal1* focal3 focal32* | quorum | 2,700 | 2,699 | 2,699 | 1 {'owner_bound': 1} | 0 | 0 | 0 | 169 | 397 | 37.5 (0.2–37.7) | 53 / 14.8 |
| 1 | a9436754 | focal4 focal12 focal20 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 490 | 0.5 (1.2–1.7) | 43 / 4.8 |
| 2 | 82d9a53f | focal2 focal7* focal30 | leader | 2,700 | 2,679 | 2,679 | 21 {'owner_bound': 16, 'ingress_capacity': 4, 'unavailable': 1} | 0 | 0 | 0 | 2 | 414 | 17.6 (22.8–40.4) | 56 / 17.8 |
| 3 | d16b4856 | focal9* focal10 focal17* | quorum | 2,700 | 2,637 | 2,637 | 63 {'unavailable': 63} | 0 | 0 | 0 | 121 | 375 | 38.1 (0.2–38.4) | 53 / 14.8 |
| 4 | d7306a1b | focal13 focal14 focal21 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 461 | 0.5 (0.4–0.9) | 44 / 5.8 |
| 5 | f0757a76 | focal6* focal11 focal16* | quorum | 2,700 | 2,699 | 2,699 | 1 {'owner_bound': 1} | 0 | 0 | 0 | 207 | 369 | 39.7 (0.1–39.9) | 60 / 21.8 |
| 6 | d1a1c25f | focal5 focal18 focal19 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 466 | 0.5 (0.9–1.4) | 43 / 4.8 |
| 7 | 074fcc52 | focal15 focal22 focal29 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 425 | 0.5 (0.5–1.1) | 43 / 4.8 |
| 8 | 047d34a6 | focal8* focal24 focal25 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 397 | 0.6 (43.0–43.6) | 44 / 5.8 |
| 9 | 8bfe87b7 | focal26 focal28* focal33 | leader | 2,700 | 2,655 | 2,655 | 45 {'owner_bound': 13, 'ingress_capacity': 32} | 0 | 0 | 0 | 2 | 487 | 16.8 (22.0–38.8) | 52 / 13.8 |
| 10 | 723e7fa7 | focal23* focal27* focal31* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 253 | 401 | 38.8 (0.2–38.9) | 66 / 27.8 |

totals: started 29,700, written 29,569, judged 29,569, refused 131 {'owner_bound': 31, 'ingress_capacity': 36, 'unavailable': 64}, unknown 0, abandoned 0, undelivered 0, resent 754; judged/started 99.559%
writes (measured window): committed 291,993, refused 131, unknown 0; committed p50/p99/p99.9/max 108.7/19627.5/35654.7/42489.5 ms; schedule delay p99 9798.1 ms; wall 136.8 s
refusals (reasons): [['access: Capacity', 36], ['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #" }', 30], ['access: Unavailable', 64], ['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #,#" }', 1]]

writes not committed, by start (5 s windows from the kill): outcome/cause=count
  [   0,   5) refused/owner_bound=5, refused/unavailable=36
  [   5,  10) refused/unavailable=27
  [  10,  15) refused/ingress_capacity=36, refused/owner_bound=25, refused/unavailable=1
  [  20,  25) refused/owner_bound=1

not committed: 131; started 0.1 to 20.7 s; latency 0.3 to 30.0 s; steps {9: 34, 1: 63, 10: 4, 4: 30}

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 1
  focal8 047d34a6: native: native lifecycle admission: a configured bound or counter would be exceeded
