## focal-220-128-work-agents500-nodes33-faults.json

killed (12): focal2 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all killed by 2.2 s, started again at 38.3 s; serving 38.7 to 44.6 s (12 of 12), 0.4 to 6.3 s after their start

| ledger | session | voters (killed *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the kill) | recovered at (s from kill / from start) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 2b179a6c | focal1 focal5 focal6* | a follower | 2,700 | 2,692 | 2,692 | 7 {'unavailable': 7} | 0 | 1 | 0 | 131 | 228 | 0.5 (0.7–1.2) | 87 / 48.7 |
| 1 | 5992e62f | focal4 focal14 focal15 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 237 | 0.5 (6.8–7.3) | 81 / 42.7 |
| 2 | 7e4375ed | focal7* focal17* focal27* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 353 | 231 | 38.0 (0.2–38.2) | 75 / 36.7 |
| 3 | 47485f5b | focal9* focal10 focal11 | a follower | 2,700 | 2,677 | 2,677 | 23 {'unavailable': 23} | 0 | 0 | 0 | 214 | 338 | 0.5 (6.6–7.1) | 61 / 22.7 |
| 4 | 966014b3 | focal13 focal26 focal33 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 251 | 0.5 (0.9–1.4) | 80 / 41.7 |
| 5 | 28f6d205 | focal8* focal12 focal16* | quorum | 2,700 | 2,695 | 2,695 | 5 {'owner_bound': 5} | 0 | 0 | 0 | 168 | 247 | 35.6 (0.2–35.8) | 83 / 44.7 |
| 6 | b7578d1f | focal19 focal29 focal30 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 224 | 0.5 (0.6–1.1) | 87 / 48.7 |
| 7 | bfaa46f9 | focal23* focal22 focal24 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 227 | 0.5 (3.3–3.8) | 77 / 38.7 |
| 8 | f26ce23f | focal3 focal25 focal32* | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 238 | 0.5 (0.6–1.1) | 54 / 15.7 |
| 9 | fc5e5302 | focal2* focal21 focal28* | quorum | 2,700 | 2,697 | 2,697 | 3 {'owner_bound': 3} | 0 | 0 | 0 | 259 | 216 | 39.7 (0.2–39.9) | 75 / 36.7 |
| 10 | 629ac469 | focal18 focal20 focal31* | leader | 2,700 | 2,672 | 2,672 | 28 {'owner_bound': 24, 'unavailable': 4} | 0 | 0 | 0 | 93 | 229 | 1.4 (0.2–1.6) | 92 / 53.7 |

totals: started 29,700, written 29,633, judged 29,633, refused 66 {'owner_bound': 32, 'unavailable': 34}, unknown 0, abandoned 1, undelivered 0, resent 1218; judged/started 99.774%
writes (measured window): committed 292,550, refused 66, unknown 0; committed p50/p99/p99.9/max 118.6/14886.2/37485.3/46177.5 ms; schedule delay p99 12.2 ms; wall 137.5 s
refusals (reasons): [['access: Unavailable', 34], ['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #" }', 29], ['lifecycle read: client: serving replica is behind; published sequence is #', 1], ['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #,#" }', 3]]

writes not committed, by start (5 s windows from the kill): outcome/cause=count
  [   0,   5) refused/owner_bound=15, refused/unavailable=1
  [   5,  10) refused/owner_bound=4, refused/unavailable=28
  [  10,  15) refused/owner_bound=8, refused/unavailable=5
  [  35,  40) refused/owner_bound=5

not committed: 66; started 0.1 to 36.7 s; latency 0.0 to 30.0 s; steps {4: 31, 10: 3, 9: 32}

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 0
