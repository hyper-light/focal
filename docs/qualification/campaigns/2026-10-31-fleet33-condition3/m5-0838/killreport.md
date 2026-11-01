## focal-220-128-work-agents500-nodes33-faults.json

killed (12): focal5 focal10 focal11 focal12 focal13 focal20 focal21 focal22 focal24 focal28 focal31 focal32; all killed by 2.9 s, started again at 68.8 s; serving 69.4 to 72.3 s (12 of 12), 0.6 to 3.5 s after their start

| ledger | session | voters (killed *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the kill) | recovered at (s from kill / from start) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 230118f2 | focal1 focal15 focal17 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 232 | 0.4 (2.4–2.8) | 90 / 21.2 |
| 1 | cb00ba7d | focal4 focal5* focal6 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 178 | 0.4 (2.4–2.8) | 96 / 27.2 |
| 2 | 7320dc77 | focal7 focal21* focal23 | a follower | 2,700 | 2,072 | 2,072 | 628 {'unavailable': 628} | 0 | 0 | 0 | 228 | 317 | 0.4 (2.4–2.8) | 86 / 17.2 |
| 3 | 5896847e | focal10* focal18 focal26 | leader | 2,700 | 2,008 | 2,008 | 692 {'owner_bound': 30, 'unavailable': 662} | 0 | 0 | 0 | 181 | 178 | 1.4 (0.2–1.7) | 96 / 27.2 |
| 4 | aa9aa933 | focal13* focal33 focal32* | quorum | 2,700 | 2,109 | 2,112 | 591 {'owner_bound': 6, 'unavailable': 585} | 0 | 0 | 0 | 394 | 221 | 69.1 (0.2–69.4) | 90 / 21.2 |
| 5 | bde05bcf | focal16 focal20* focal27 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 262 | 0.4 (2.4–2.8) | 88 / 19.2 |
| 6 | f9043859 | focal3 focal11* focal19 | a follower | 2,700 | 2,039 | 2,039 | 661 {'unavailable': 661} | 0 | 0 | 0 | 224 | 305 | 0.4 (2.4–2.7) | 91 / 22.2 |
| 7 | ab725a5a | focal14 focal22* focal30 | leader | 2,700 | 1,984 | 1,984 | 716 {'owner_bound': 29, 'unavailable': 687} | 0 | 0 | 0 | 226 | 255 | 1.9 (0.2–2.1) | 88 / 19.2 |
| 8 | f704e3a8 | focal2 focal9 focal25 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 238 | 0.4 (2.4–2.8) | 89 / 20.2 |
| 9 | 1e3a4a3d | focal8 focal12* focal28* | quorum | 2,700 | 2,119 | 2,120 | 581 {'owner_bound': 3, 'unavailable': 578} | 0 | 0 | 0 | 359 | 213 | 68.4 (0.2–68.6) | 90 / 21.2 |
| 10 | 4ada1b22 | focal24* focal29 focal31* | quorum | 2,700 | 2,138 | 2,140 | 562 {'owner_bound': 7, 'unavailable': 555} | 0 | 0 | 0 | 419 | 275 | 68.8 (0.2–69.0) | 88 / 19.2 |

totals: started 29,700, written 25,269, judged 25,275, refused 4,431 {'owner_bound': 75, 'unavailable': 4356}, unknown 0, abandoned 0, undelivered 0, resent 2031; judged/started 85.101%
writes (measured window): committed 253,740, refused 4,431, unknown 0; committed p50/p99/p99.9/max 179.9/22153.0/60461.6/73303.4 ms; schedule delay p99 49.0 ms; wall 137.9 s
refusals (reasons): [['access: Unavailable', 4356], ['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #" }', 70], ['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #,#" }', 5]]

writes not committed, by start (5 s windows from the kill): outcome/cause=count
  [   0,   5) refused/owner_bound=38, refused/unavailable=524
  [   5,  10) refused/owner_bound=2, refused/unavailable=434
  [  10,  15) refused/owner_bound=30, refused/unavailable=751
  [  15,  20) refused/unavailable=529
  [  20,  25) refused/unavailable=529
  [  25,  30) refused/unavailable=553
  [  30,  35) refused/unavailable=556
  [  35,  40) refused/unavailable=441
  [  40,  45) refused/unavailable=39
  [  65,  70) refused/owner_bound=4
  [  70,  75) refused/owner_bound=1

not committed: 4431; started 0.1 to 70.9 s; latency 0.0 to 60.7 s; steps {4: 2561, 10: 86, 9: 75, 11: 6, 5: 7, 1: 1674, 6: 4, 2: 6, 3: 4, 8: 6, 7: 2}

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 0
